/*
Note: The VOPR Hub must run a VOPR in a separate tigerbeetle directory to prevent it from checking
out a commit that could change the hub itself
In order to parse the output correctly the VOPR must run from within the tigerbeetle directory
To run the VOPR Hub, Zig must be installed and five environmental variables are required:
1. TIGERBEETLE_DIRECTORY the location of the VOPR's tigerbeetle directory
2. ISSUE_DIRECTORY where issues are stored on disk
3. DEVELOPER_TOKEN for access to GitHub
4. VOPR_HUB_ADDRESS for the IP address to listen on for incoming messages
5. REPOSITORY_URL to post the GitHub issue
*/

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
	"strconv"
)

const max_concurrent_connections = 4
const max_queuing_messages = 100
const length_of_vopr_message = 45
const max_length_of_vopr_output = 8 * 1 << 25

// GitHub's hard character limit for issues is 65536
const max_github_issue_size = 60000

const usageFmt = `usage: %s [flags]

environment:
  TIGERBEETLE_DIRECTORY  the location of the VOPR's tigerbeetle directory
  ISSUE_DIRECTORY        where issues are stored on disk
  DEVELOPER_TOKEN        required for making GitHub API calls
  VOPR_HUB_ADDRESS       the IP address to listen on for incoming bug reports
  REPOSITORY_URL         TigerBeetle's GitHub repository URL

flags:
  -debug  enabled debug logging
`

var (
	debug_mode            bool
	tigerbeetle_directory string
	issue_directory       string
	developer_token       string
	vopr_hub_address      string
	repository_url        string
	whitespace_regexpr    = regexp.MustCompile(`( ( )+)|(\t)+`)
)

type Branch struct {
	Name string `json:"name"`
}

type Label struct {
	Name string `json:"name"`
}

type Issue struct {
	Labels []Label `json:"labels"`
}

// An alias for the vopr_message's byte array.
type vopr_message_byte_array [length_of_vopr_message]byte

type bug_type uint8

const (
	bug_type_correctness = iota + 1
	bug_type_liveness
	bug_type_crash
)

type vopr_message struct {
	bug    bug_type
	seed   uint64
	commit [20]byte
	hash   [16]byte
}

// The VOPR's output is stored in a vopr_output struct where certain elements can be extracted and
// processed.
// parameters includes information about the conditions under which TigerBeetle is run.
type vopr_output struct {
	logs             []byte
	stack_trace      []byte
	stack_trace_hash string
	parameters       string
	seed_passed      bool
}

// Checks if the simulator passed or the bug was confirmed.
func (output *vopr_output) test_passed() bool {
	return strings.Contains(string(output.logs), "PASSED")
}

// The stack trace is moved from output.logs into output.stack_trace.
func (output *vopr_output) extract_stack_trace(message *vopr_message) {
	// Only extract stack traces for crash bugs and panic statements for correctness bugs.
	if output.seed_passed || message.bug == bug_type_liveness {
		return
	}
	// The stack trace begins on the first line that:
	// - doesn't begin with a bracket (e.g. "[debug] ...",
	// - doesn't begin with a number (e.g. "10   \ .       533V ...")
	// - doesn't begin with whitespace (e.g. " SEED=3262606282417516824")
	address_regexpr := regexp.MustCompile(`(\n([^\[\s\d]))`)
	index := address_regexpr.FindIndex(output.logs)
	// If an instance is found then the index returns an int array containing the start and end
	// index for the match.
	if len(index) == 2 {
		output.stack_trace = output.logs[index[0]:]
		output.logs = output.logs[:index[0]]
		log_debug("Stack trace has been extracted", message.hash[:])
	} else {
		log_debug("No stack trace was found", message.hash[:])
	}
}

// The VOPR's parameters are moved from output.logs into output.parameters.
func (output *vopr_output) extract_parameters(message *vopr_message) {
	state_regexpr := regexp.MustCompile(`\[info\] \(cluster\):[^\[]+`)
	index := state_regexpr.FindIndex(output.logs)
	// If an instance is found then the index returns an int array containing the start and end
	// index for the match.
	if len(index) == 2 {
		output.parameters = string(output.logs[index[0]:index[1]])
		output.parameters = strings.TrimSpace(output.parameters)
		output.logs = append(output.logs[:index[0]], output.logs[index[1]:]...)
		log_debug("The VOPR's parameters have been extracted", message.hash[:])
	} else {
		output.parameters = ""
		log_debug("No VOPR parameters were found", message.hash[:])
	}
}

// The stack traced is parsed so that all unique paths and memory addresses are removed.
// If the entire stack trace has been recorded, then it's hash will be deterministic.
func (output *vopr_output) parse_stack_trace(message *vopr_message) {
	if len(output.stack_trace) > 0 {
		path_regexpr := regexp.MustCompile(`(/.*)+/tigerbeetle/`)
		memory_address_regexpr := regexp.MustCompile(`: 0x[0-9a-z]* in`)
		thread_regexpr := regexp.MustCompile(`thread [0-9]* panic:`)
		line_regexpr := regexp.MustCompile(`line [0-9]*: [0-9]*`)
		output.stack_trace = path_regexpr.ReplaceAll(output.stack_trace, []byte(""))
		output.stack_trace = thread_regexpr.ReplaceAll(output.stack_trace, []byte("thread panic:"))
		output.stack_trace = line_regexpr.ReplaceAll(output.stack_trace, []byte(""))
		output.stack_trace = memory_address_regexpr.ReplaceAll(output.stack_trace, []byte(": in"))
		log_debug("The stack trace has been parsed", message.hash[:])
	} else {
		log_debug("No stack trace was found to parse", message.hash[:])
	}
}

// Assigns a SHA256 checksum, or a null string, to output.stack_trace_hash.
func (output *vopr_output) hash_stack_trace(message *vopr_message) {
	// Check if there is a stack trace
	if len(output.stack_trace) > 0 {
		check_sum := sha256.Sum256(output.stack_trace)
		output.stack_trace_hash = fmt.Sprintf("%x", check_sum)
		log_debug("The stack trace has been hashed: "+output.stack_trace_hash, message.hash[:])
	} else {
		output.stack_trace_hash = ""
		log_debug("No stack trace was found to hash", message.hash[:])
	}
}

// Creates the string that forms the body of the GitHub issue.
func (output *vopr_output) create_issue_markdown(message vopr_message) string {
	branches := get_branch_names(hex.EncodeToString(message.commit[:]), message.hash[:])

	start := time.Now()
	run_vopr_ReleaseSafe(message)
	t := time.Now()
	duration := t.Sub(start).Round(time.Second)

	// Limit set here to avoid writing only a few characters for any particular section.
	const min_useful_length = 100

	// Extract the information about the conditions under which the VOPR runs TigerBeetle.
	output.extract_parameters(&message)

	// Remove white space.
	output.stack_trace = whitespace_regexpr.ReplaceAll(output.stack_trace, []byte(""))
	output.logs = whitespace_regexpr.ReplaceAll(output.logs, []byte(""))
	output.parameters = strings.ReplaceAll(output.parameters, " ", "")

	length_of_stack_trace := len(output.stack_trace)
	length_of_parameters := len(output.parameters)
	length_of_logs := len(output.logs)
	remaining_space := max_github_issue_size

	stack_trace := ""
	parameters := ""
	debug_logs := ""

	// Set the stack trace string.
	if length_of_stack_trace > 0 {
		if length_of_stack_trace <= max_github_issue_size {
			stack_trace = make_markdown_compatible(
				"\n```" + string(output.stack_trace[:]) + "\n```\n",
			)
			remaining_space -= length_of_stack_trace
		} else {
			// If the stack trace is too large then just capture the beginning of it.
			stack_trace = make_markdown_compatible(
				"\n```" + string(output.stack_trace[:max_github_issue_size-4]) + "\n```\n",
			)
			stack_trace += "..."
			remaining_space = 0
		}
	}

	// Set the parameters string.
	if length_of_parameters > 0 && remaining_space >= min_useful_length {
		if length_of_parameters <= remaining_space {
			parameters = make_markdown_compatible(
				"\n```" + output.parameters[:] + "\n```\n",
			)
			remaining_space -= length_of_parameters
		} else {
			// If the parameters section is too large then just capture the beginning of it.
			parameters = make_markdown_compatible(
				"\n```"+output.parameters[:remaining_space-4]+"\n```\n",
			) + "..."
			remaining_space = 0
		}
	}

	// Set the debug logs string.
	if remaining_space >= min_useful_length {
		if length_of_logs < remaining_space {
			debug_logs = make_markdown_compatible(
				"\n```" + string(output.logs[:]) + "\n```\n",
			)
		} else {
			// Get the tail of the logs.
			tail_start_index := length_of_logs - remaining_space
			debug_logs = make_markdown_compatible(
				"\n```" + string(output.logs[tail_start_index:]) + "\n```\n",
			)
		}
	}

	var body string
	if branches != "" {
		body += fmt.Sprintf(
			"<strong>Branches:</strong> %s<br><br>",
			branches,
		)
	} else {
		body += "<strong>Branches:</strong> (unknown branch)<br><br>"
	}
	body += fmt.Sprintf(
		"<strong>Duration to run seed in ReleaseSafe mode:</strong> %s<br><br>",
		duration,
	)
	if len(parameters) > 0 {
		body += fmt.Sprintf(
			"<strong>Parameters:</strong><br>%s<br>",
			parameters,
		)
	}
	if len(stack_trace) > 0 {
		body += fmt.Sprintf(
			"<strong>Stack Trace:</strong><br>%s<br>",
			stack_trace,
		)
	}
	if len(debug_logs) > 0 {
		body += fmt.Sprintf(
			"<strong>Debug Logs:</strong><br><details><summary>Tail</summary><br>%s</details>",
			debug_logs,
		)
	}

	markdown := fmt.Sprintf(
		"<strong>Commit:</strong> ```%s``` <br><br>%s",
		hex.EncodeToString(message.commit[:]),
		body,
	)
	return markdown
}

// The limited buffer ensures that only a specified number of bytes can be written to the buffer.
type size_limited_buffer struct {
	byte_budget int
	// The buffer being wrapped
	buffer *bytes.Buffer
	// This channel is used to keep track of when the VOPR reaches its maximum allowed output.
	size_reached chan bool
}

func usage() {
	fmt.Fprintf(os.Stderr, usageFmt, os.Args[0])
	os.Exit(1)
}

// The limited buffer honours the Write function in such a way that only a specified number of
// bytes can be written to the buffer. If the maximum number of bytes has been written then no
// more will be added and a value of true is passed to the size_reached channel which acts as a
// signal to kill the process that is doing the writing.
func (limited_buffer *size_limited_buffer) Write(byte_array []byte) (int, error) {
	space_left := limited_buffer.byte_budget
	limited_buffer.byte_budget -= len(byte_array)
	if limited_buffer.byte_budget <= 0 {
		if space_left > 0 {
			bytes_written, err := limited_buffer.buffer.Write(byte_array[0 : space_left-1])
			limited_buffer.size_reached <- true
			return bytes_written, err
		} else {
			limited_buffer.size_reached <- true
			return 0, nil
		}
	} else if space_left > 0 {
		return limited_buffer.buffer.Write(byte_array)
	} else {
		return 0, nil
	}
}

func do_github_request(method, path string, content, hash []byte) *http.Response {
	for {
		var body io.Reader = nil
		if content != nil {
			body = strings.NewReader(string(content))
		}

		req, err := http.NewRequest(method, path, body)
		if err != nil {
			log_error("Failed to create the HTTP request for the GitHub API", hash)
			panic(err.Error())
		}

		req.Header.Set("Authorization", "token "+developer_token)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			log_error("Failed to send the HTTP request for the GitHub API", hash)
			panic(err.Error())
		}

		if res.StatusCode == 403 && res.Header.Get("x-ratelimit-limit") == "0" {
			res.Body.Close()

			reset_at := res.Header.Get("x-ratelimit-reset")
			expires, err := strconv.ParseInt(reset_at, 10, 64)
			if err != nil {
				log_error(fmt.Sprintf("Failed to parse rate limit reset %s", reset_at), hash)
				panic(err.Error())
			}
			
			deadline := time.Unix(expires, 0)
			time.Sleep(deadline.Sub(time.Now()))
			continue
		}

		return res
	}
}

func get_branch_names(commit_sha string, message_hash []byte) string {
	branches := []Branch{}
	var branch_names string

	res := do_github_request(
		"GET",
		fmt.Sprintf("%s/commits/%s/branches-where-head", repository_url, commit_sha),
		nil,
		message_hash,
	)

	body, err := io.ReadAll(res.Body)
	res.Body.Close()

	if res.StatusCode > 299 {
		log_error(
			fmt.Sprintf(
				"Response failed with status code: %d and\nbody: %s\n",
				res.StatusCode,
				body,
			),
			message_hash,
		)
		// If there is an error instead of panicking simply return a null string to indicate that
		// no branch was found.
		// Getting a 422 error when the commit sha doesn't match any head commits.
		return ""
	}
	if err != nil {
		log_error("unable to receive a response from GitHub", message_hash)
		panic(err.Error())
	}

	err = json.Unmarshal(body, &branches)
	if err != nil {
		log_error("unable to unmarshall json", message_hash)
		panic(err.Error())
	}

	if len(branches) > 0 {
		for _, value := range branches {
			branch_names += value.Name + ", "
		}
		return strings.TrimSuffix(branch_names, ", ")
	}

	return ""
}

func run_vopr_ReleaseSafe(message vopr_message) {
	// Runs in ReleaseSafe mode
	log_info("Running the VOPR in ReleaseSafe mode...", message.hash[:])
	cmd := exec.Command(
		"zig/zig",
		"build",
		"vopr",
		"--",
		fmt.Sprintf("--seed=%d", message.seed),
		"--build-mode=ReleaseSafe",
	)
	cmd.Dir = tigerbeetle_directory
	err := cmd.Run()
	if err != nil {
		log_error("Failed to run the VOPR in ReleaseSafe mode", message.hash[:])
		panic(err.Error())
	}
}

func get_bug_name(bug bug_type) string {
	var bug_string string
	switch bug {
	case bug_type_correctness:
		bug_string = "correctness"
	case bug_type_liveness:
		bug_string = "liveness"
	case bug_type_crash:
		bug_string = "crash"
	default:
		panic("unreachable")
	}

	return bug_string
}

// Reads incoming messages (ensuring that only the correct number of bytes are read), decodes and
// validates them before adding the messages to the vopr_message_channel.
func handle_connection(
	track_connections chan bool,
	connection net.Conn,
	vopr_message_channel chan vopr_message,
) {
	// When this function completes, close the connection and decrement the connections count.
	defer func() {
		connection.Close()
		<-track_connections
		log_debug("Connection closed", nil)
	}()

	var input vopr_message_byte_array

	// If too few bytes were sent then the connection's read deadline will timeout.
	total_bytes_read := 0
	for total_bytes_read < length_of_vopr_message {
		bytes_read, error := connection.Read(input[total_bytes_read:])
		if error != nil {
			error_message := fmt.Sprintf("Client closed unexpectedly: %s", error.Error())
			log_error(error_message, nil)
			return
		}
		total_bytes_read += bytes_read
	}

	if total_bytes_read != length_of_vopr_message {
		log_error("The input was longer or shorter than expected", nil)
		return
	}

	// Decodes the byte array into a vopr_message
	message, decoding_error := decode_message(input)

	if decoding_error == nil {
		log_message := fmt.Sprintf(
			"bug: %d commit: %x seed: %d",
			message.bug,
			message.commit,
			message.seed,
		)
		log_info(log_message, message.hash[:])

		// Checks if there is capacity to process the message.
		select {
		case vopr_message_channel <- message:
			// Reply to client only reply if everything is as expected.
			_, error := connection.Write([]byte("1"))
			if error != nil {
				error_message := fmt.Sprintf("Unable to reply to client: %s", error.Error())
				log_error(error_message, message.hash[:])
				return
			}
		default:
			log_info("Too many messages already queued, dropping message", message.hash[:])
		}
	} else {
		log_error(decoding_error.Error(), nil)
	}
}

// Decodes the vopr_message_byte_array into a vopr_message struct.
func decode_message(input vopr_message_byte_array) (vopr_message, error) {
	var message vopr_message
	error := fmt.Errorf("The input received is invalid")

	if len(input) != length_of_vopr_message {
		return message, error
	}

	// Expect the first 16 bytes of the message to be the first half of a SHA256 hash of the
	// remainder of the message.
	// If correct, this half of the hash is then also used as a unique identifier of this message
	// throughout the logs.
	hash := sha256.Sum256(input[16:])
	copy(message.hash[:], hash[0:16])
	if bytes.Compare(message.hash[:], input[0:16]) != 0 {
		checksum_error := fmt.Errorf("Received message with invalid checksum")
		return message, checksum_error
	}

	// Ensure the bug type is valid.
	if input[16] != 1 && input[16] != 2 && input[16] != 3 {
		return message, error
	}
	seed := binary.BigEndian.Uint64(input[17:25])

	// The bug type (1, 2, or 3) is encoded as a uint8.
	message.bug = bug_type(input[16])
	// The seed is encoded as a uint64.
	message.seed = seed
	// The GitHub commit hash is remains as a 20 byte array.
	copy(message.commit[:], input[25:45])

	return message, nil
}

// Reads and processes messages from the vopr_message_channel channel as they arrive.
// Messages are only sent here after being decoded and validated.
func worker(vopr_message_channel chan vopr_message) {
	for message := range vopr_message_channel {
		process(message)
		log_info("Message processing complete", message.hash[:])
	}
}

// Responsable for running the VOPR, capturing the output, processing it, writing it to file and
// making the GitHub issue.
// It also checks for duplicate issues and ensures the specified commit is available.
func process(message vopr_message) {
	// Bugs 1 & 2 don't require a stack trace to be deduped.
	if is_duplicate_bug_1_and_2(message) {
		return
	}

	// Get the number of issues opened by the vopr_hub already.
	num_issues := get_open_issue_count()
	// If there are 6 or more VOPR created issues on GitHub then don't continue message processing.
	if num_issues >= 6 {
		log_info("There are too many open GitHub issues.", message.hash[:])
		return
	}

	commit_string := hex.EncodeToString(message.commit[:])

	error := checkout_commit(commit_string, message.hash[:])
	if error == nil {
		log_debug("Successfully checked out commit "+commit_string, message.hash[:])
	} else {
		error_message := fmt.Sprintf(
			"Failed to checkout commit %s: %s", commit_string,
			error.Error(),
		)
		log_error(error_message, message.hash[:])
		return
	}

	var output vopr_output
	run_vopr(message.seed, &output, message.hash[:])
	if output.test_passed() {
		log_error("The seed unexpectedly passed", message.hash[:])
		output.seed_passed = true
	} else {
		output.seed_passed = false
	}

	output.extract_stack_trace(&message)
	output.parse_stack_trace(&message)
	output.hash_stack_trace(&message)

	issue_file_name := generate_file_name(message, output.stack_trace_hash)

	// Bug 3 requires a stack trace to be deduped
	if message.bug == bug_type_crash && is_duplicate(issue_file_name, message.hash[:]) {
		return
	}

	// Saves report to disk
	create_issue_file(issue_file_name, &output, message.hash[:])

	err := create_github_issue(message, &output)
	if err != nil {
		log_error(
			fmt.Sprintf("Failed to create GitHub issue: %s", err.Error()),
			message.hash[:],
		)
		return
	}
}

// Checks if a duplicate issue has already been submitted.
func is_duplicate(issue_file_name string, message_hash []byte) bool {
	if _, error := os.Stat(issue_file_name); error == nil {
		log_info("Duplicate issue found", message_hash)
		return true
	} else if errors.Is(error, fs.ErrNotExist) {
		return false
	} else {
		// This is an actual error from the OS/FS. The file may or may not exist, we don't know.
		log_error(error.Error(), message_hash)
		panic(error.Error())
	}
}

// Bugs 1 and 2 don't require a stack trace for deduping and so they can be deduped before the VOPR
// is run.
func is_duplicate_bug_1_and_2(message vopr_message) bool {
	// If bug type 1 or 2 first check if file exists before parsing and hashing the stack trace.
	if message.bug == bug_type_correctness || message.bug == bug_type_liveness {
		issue_file_name := generate_file_name(message, "")
		return is_duplicate(issue_file_name, message.hash[:])
	} else {
		return false
	}
}

func get_open_issue_count() int {
	issues := []Issue{}

	res := do_github_request(
		"GET",
		repository_url+"/issues?state=open&creator=tigerbeetle-vopr",
		nil,
		nil,
	)

	body, err := io.ReadAll(res.Body)
	res.Body.Close()

	if res.StatusCode > 299 {
		log_error(fmt.Sprintf(
			"Response failed with status code: %d and\nbody: %s\n",
			res.StatusCode,
			body,
		), nil)
		panic(err.Error())
	}
	if err != nil {
		log_error("unable to receive a response from GitHub", nil)
		panic(err.Error())
	}

	err = json.Unmarshal(body, &issues)
	if err != nil {
		log_error("unable to unmarshall json", nil)
		panic(err.Error())
	}
	return len(issues)
}

// Filenames are used to uniquely identify issues for deduping purposes.
// correctness: 1_seed_commithash
// liveness: 2_seed_commithash
// crash: 3_commithash_stacktracehash
func generate_file_name(message vopr_message, stack_trace_hash string) string {
	commit_string := hex.EncodeToString(message.commit[:])

	if message.bug == bug_type_correctness || message.bug == bug_type_liveness {
		return fmt.Sprintf(
			"%s/%d_%d_%s",
			issue_directory,
			message.bug,
			message.seed,
			commit_string,
		)
	} else if message.bug == bug_type_crash {
		return fmt.Sprintf(
			"%s/%d_%s_%s",
			issue_directory,
			message.bug,
			commit_string,
			stack_trace_hash,
		)
	} else {
		panic("unreachable")
	}
}

// Fetch available branches from GitHub and checkout the correct commit if it exists.
func checkout_commit(commit string, message_hash []byte) error {
	// Ensures commit is all hexadecimal.
	commit_valid, error := regexp.MatchString(`^([0-9a-f]){40}$`, commit)
	if error != nil {
		panic(error)
	} else if !commit_valid {
		error = fmt.Errorf("The GitHub commit contained unexpected characters")
		log_error(error.Error(), message_hash)
		return error
	}

	// Git commands need to be run with the TigerBeetle directory as their working_directory.
	fetch_command := exec.Command("git", "fetch", "--all")
	fetch_command.Dir = tigerbeetle_directory
	error = fetch_command.Run()
	if error != nil {
		error_message := fmt.Sprintf("Failed to run git fetch: %s", error.Error())
		log_error(error_message, message_hash)
		return error
	}

	// Checkout the commit specified in the vopr_message.
	checkout_command := exec.Command("git", "checkout", commit)
	checkout_command.Dir = tigerbeetle_directory
	error = checkout_command.Run()
	if error != nil {
		error_message := fmt.Sprintf("Failed to run git checkout: %s", error.Error())
		log_error(error_message, message_hash)
		return error
	}

	// Inspect the git logs.
	log_command := exec.Command("git", "log", "-1")
	log_command.Dir = tigerbeetle_directory
	log_output := make([]byte, 47)
	log_output, error = log_command.Output()
	if error != nil {
		error_message := fmt.Sprintf("Failed to run git log: %s", error.Error())
		log_error(error_message, message_hash)
		return error
	}

	// Check the log to determine if the commit has been successfully checked out.
	current_commit := string(log_output[0:47])
	checkout_successful, error := regexp.MatchString("^commit "+commit, current_commit)
	if error != nil {
		error_message := fmt.Sprintf(
			"Regular expression failure while checking the git logs: %s",
			error.Error(),
		)
		log_error(error_message, message_hash)
		return error
	}

	if !checkout_successful {
		error = fmt.Errorf("Checkout failed")
		return error
	}

	return nil
}

// The VOPR is run from the TigerBeetle directory and its output is captured.
func run_vopr(seed uint64, output *vopr_output, message_hash []byte) {
	// Create a limited_buffer to read the VOPR output
	var vopr_std_err_buffer bytes.Buffer
	limited_buffer := size_limited_buffer{
		buffer:       &vopr_std_err_buffer,
		byte_budget:  int(max_length_of_vopr_output),
		size_reached: make(chan bool),
	}

	// The channel monitors if the VOPR completes before the maximum output is reached.
	vopr_completed := make(chan error)

	// Runs in Debug mode
	log_info("Running the VOPR in Debug mode...", message_hash)
	cmd := exec.Command(
		"zig/zig",
		"build",
		"vopr",
		"--",
		fmt.Sprintf("--seed=%d", seed),
	)
	cmd.Dir = tigerbeetle_directory
	cmd.Stderr = &limited_buffer
	// Start() runs asynchronously but is used because it allows a process to be killed.
	error := cmd.Start()
	if error != nil {
		log_error("Failed to start the VOPR in Debug mode", message_hash)
		panic(error.Error())
	}

	// Wait() runs synchronously. A separate Goroutine is needed to prevent the code from blocking.
	// The VOPR might end prematurely instead if its output exceeds the maximum space.
	go func() {
		result := cmd.Wait()
		vopr_completed <- result
	}()

	// The code blocks until a value is found in either the vopr_completed or size_reached channel.
	select {
	case result := <-vopr_completed:
		if result != nil {
			log_error("The VOPR did not complete successfully", message_hash)
		} else {
			log_info("The VOPR completed successfully", message_hash)
		}
	case max_size := <-limited_buffer.size_reached:
		if max_size {
			cmd.Process.Kill()
		}
		log_info("The VOPR has completed with a liveness bug", message_hash)
	}

	// All results are stored in the output.logs byte array.
	output.logs = limited_buffer.buffer.Bytes()
}

// Writes the debug logs and parsed stack trace to a file on disk.
func create_issue_file(issue_file_name string, output *vopr_output, message_hash []byte) {
	full_output := append(output.logs, output.stack_trace...)
	error := os.WriteFile(issue_file_name, full_output, 0666)
	if error != nil {
		error_message := fmt.Sprintf("Failed to write to file: %s", error.Error())
		log_error(error_message, message_hash)
		panic(error.Error())
	} else {
		log_message := fmt.Sprintf("Created file: %s", issue_file_name)
		log_info(log_message, message_hash)
	}
}

// Submits a GitHub issue that includes the debug logs and parsed stack trace.
func create_github_issue(message vopr_message, output *vopr_output) error {
	body := output.create_issue_markdown(message)
	if output.seed_passed {
		body = "Note this seed passed when it was rerun by the VOPR Hub.<br><br>" + body
	}

	bug_string := get_bug_name(message.bug)
	title := fmt.Sprintf(
		"%s%s: %d",
		strings.ToUpper(string(bug_string[0])),
		string(bug_string[1:]),
		message.seed,
	)

	issue_contents, error := json.Marshal(struct {
		Title  string   `json:"title"`
		Body   string   `json:"body"`
		Labels []string `json:"labels"`
	}{
		Title:  title,
		Body:   body,
		Labels: []string{},
	})

	if error != nil {
		return error
	}

	post_response := do_github_request(
		"POST",
		repository_url+"/issues",
		issue_contents,
		message.hash[:],
	)

	defer post_response.Body.Close()

	if post_response.StatusCode < 200 || post_response.StatusCode > 299 {
		error_message := fmt.Sprintf(
			"Received a non 2xx status code from GitHub. StatusCode: %d. Status: %s",
			post_response.StatusCode,
			post_response.Status,
		)
		log_error("Failed to get a reply from the GitHub API", message.hash[:])
		return fmt.Errorf(error_message)
	}

	log_info(
		fmt.Sprintf(
			"GitHub issue has been created. Received StatusCode: %d and Status: %s.",
			post_response.StatusCode,
			post_response.Status,
		),
		message.hash[:],
	)
	return nil
}

// Escape characters that have special use in Markdown.
func make_markdown_compatible(text string) string {
	text = strings.ReplaceAll(text, "^", "")
	text = strings.ReplaceAll(text, "\"", "\\\"")
	text = strings.ReplaceAll(text, "\t", "")
	return text
}

func load_environment_variable(name string) string {
	val, _ := os.LookupEnv(name)
	if val == "" {
		log_error(fmt.Sprintf("env %s not set", name), nil)
		usage()
		os.Exit(1)
	} else if name == "DEVELOPER_TOKEN" {
		log_debug(fmt.Sprintf("env %s=*****", name), nil)
	} else {
		log_debug(fmt.Sprintf("env %s=%q", name, val), nil)
	}
	return val
}

// Ensures that a variable isn't empty or only white space.
func not_empty(variable *string) bool {
	*variable = strings.TrimSpace(*variable)
	if *variable == "" {
		return false
	} else {
		return true
	}
}

func log_error(message string, vopr_message_hash []byte) {
	log_message("error: ", message, vopr_message_hash)
}

func log_debug(message string, vopr_message_hash []byte) {
	if debug_mode {
		log_message("debug: ", message, vopr_message_hash)
	}
}

func log_info(message string, vopr_message_hash []byte) {
	log_message("info:  ", message, vopr_message_hash)
}

// Formats all the log messages and adds a timestamp to them.
func log_message(log_level string, message string, vopr_message_hash []byte) {
	// Gets the current time in UTC.
	timestamp := time.Now().UTC().Format(time.RFC3339)
	if vopr_message_hash != nil {
		// Only use the first 16 bytes of the message hash as the ID.
		fmt.Printf("%s %sID:%x %s\n", timestamp, log_level, vopr_message_hash, message)
	} else {
		fmt.Printf("%s %s%s\n", timestamp, log_level, message)
	}
}

func main() {
	// Determine the mode in which to run the VOPR Hub.
	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flags.BoolVar(&debug_mode, "debug", false, "runs with debugging logs enabled")
	if err := flags.Parse(os.Args[1:]); err != nil {
		usage()
	}

	// Retrieve all the required environment variables up front.
	tigerbeetle_directory = load_environment_variable("TIGERBEETLE_DIRECTORY")
	issue_directory = load_environment_variable("ISSUE_DIRECTORY")
	developer_token = load_environment_variable("DEVELOPER_TOKEN")
	vopr_hub_address = load_environment_variable("VOPR_HUB_ADDRESS")
	repository_url = load_environment_variable("REPOSITORY_URL")

	// This channel ensures no more than max_concurrent_connections are being handled at one time.
	track_connections := make(chan bool, max_concurrent_connections)

	// The channel will receieve fixed-size byte arrays from a VOPR.
	vopr_message_channel := make(chan vopr_message, max_queuing_messages)

	// Create a worker Goroutine to call process on each item as it appears in the channel.
	go worker(vopr_message_channel)

	listener, error := net.Listen("tcp", vopr_hub_address)
	if error != nil {
		error_message := fmt.Sprintf("Could not listen on %s: %s", vopr_hub_address, error.Error())
		log_error(error_message, nil)
		os.Exit(1)
	}
	defer listener.Close()
	log_info("Listening...", nil)

	for {
		connection, error := listener.Accept()
		connection.SetReadDeadline(time.Now().Add(10 * time.Second))
		if error != nil {
			error_message := fmt.Sprintf(
				"Unable to set read deadline on the connection: %s",
				error.Error(),
			)
			log_error(error_message, nil)
			connection.Close()
			continue
		}

		select {
		case track_connections <- true:
			go handle_connection(track_connections, connection, vopr_message_channel)
		default:
			log_info(
				"Connection closed because there are currently too many open connections",
				nil,
			)
			connection.Close()
		}
	}
}
