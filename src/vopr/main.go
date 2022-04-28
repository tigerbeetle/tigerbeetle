/*
Note: The VOPR Hub should not be run from within the tigerbeetle directory
However, to parse the output correctly the VOPR must run from within the tigerbeetle directory
To run the VOPR Hub Zig must be installed and five environmental variables are required:
1. TIGERBEETLE_DIRECTORY where TigerBeetle is stored.
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
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
)

const MAX_CONCURRENT_CONNECTIONS = 4
const MAX_QUEUING_MESSAGES = 100
const LENGTH_OF_VOPR_MESSAGE = 29

// GitHub's hard character limit for issues is 65536
const MAX_GITHUB_ISSUE_SIZE = 60000

var debug_mode bool
var max_length_of_vopr_output = 8 * math.Pow(2, 20)
var tigerbeetle_directory string
var issue_directory string
var developer_token string
var vopr_hub_address string
var repository_url string

// An alias for the vopr_message's byte array
type vopr_message_byte_array [LENGTH_OF_VOPR_MESSAGE]byte

// Struct for decoded VOPR message
// bug type 1 - correctness
// bug type 2 - liveness
// bug type 3 - crash
type vopr_message struct {
	bug    uint8
	seed   uint64
	commit [20]byte
	hash   [32]byte
}

// The VOPR's output is stored in a vopr_output struct where certain elements can be extracted and
// processed.
// parameters includes information about the conditions under which TigerBeetle is run.
type vopr_output struct {
	logs             []byte
	stack_trace      []byte
	stack_trace_hash string
	parameters       string
}

// The stack trace is moved from output.logs into output.stack_trace.
func (output *vopr_output) extract_stack_trace(message *vopr_message) {
	// The stack trace begins on the first line that starts with neither a square bracket nor
	// white space.
	address_regexpr := regexp.MustCompile(`(^([^\[\s]))|(\n([^\[\s]))`)
	index := address_regexpr.FindIndex(output.logs)
	// If an instance is found then the index returns an int array containing the start and end
	// index for the match
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
	state_regexpr := regexp.MustCompile(`\[info\] \(state_checker\):[^\[]+`)
	index := state_regexpr.FindIndex(output.logs)
	// If an instance is found then the index returns an int array containing the start and end
	// index for the match
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
		path_regexpr := regexp.MustCompile(`(/([A-Za-z])*)+/tigerbeetle/`)
		memory_address_regexpr := regexp.MustCompile(`: 0x([0-9a-z])* in`)
		thread_regexpr := regexp.MustCompile(`thread ([0-9])* panic:`)
		line_regexpr := regexp.MustCompile(`line ([0-9])*: ([0-9])*`)
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

// The limited buffer ensures that only a specified number of bytes can be written to the buffer.
type size_limited_buffer struct {
	byte_budget int
	// The buffer being wrapped
	buffer *bytes.Buffer
	// This channel is used to keep track of when the VOPR reaches its maximum allowed output
	size_reached chan bool
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

// Reads incoming messages (ensuring that only the correct number of bytes are read), decodes and
// validates them before adding the messages to the vopr_message_channel.
func handle_connection(track_connections chan bool, connection net.Conn, vopr_message_channel chan vopr_message) {
	// When this function completes, close the connection and decrement the connections count.
	defer func() {
		connection.Close()
		<-track_connections
		log_debug("Connection closed", nil)
	}()

	var input vopr_message_byte_array

	// If too few bytes were sent the read will timeout because a read deadline was set on the connection.
	total_bytes_read := 0
	for total_bytes_read < LENGTH_OF_VOPR_MESSAGE {
		bytes_read, error := connection.Read(input[total_bytes_read:])
		if error != nil {
			error_message := fmt.Sprintf("Client closed unexpectedly: %s", error.Error())
			log_error(error_message, nil)
			return
		}
		total_bytes_read += bytes_read
	}

	if total_bytes_read != LENGTH_OF_VOPR_MESSAGE {
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

		// Checks if there is capacity to process the message
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
		log_info("The input received is invalid", nil)
	}
}

// Decodes the vopr_message_byte_array into a vopr_message struct.
func decode_message(input vopr_message_byte_array) (vopr_message, error) {
	var message vopr_message
	error := fmt.Errorf("Invalid input")

	if len(input) != LENGTH_OF_VOPR_MESSAGE {
		log_error(error.Error(), nil)
		return message, error
	}

	// Ensure the bug and seed are valid.
	if !(input[0] == 1 || input[0] == 2 || input[0] == 3) {
		log_error(error.Error(), nil)
		return message, error
	}
	seed := uint64(binary.BigEndian.Uint64(input[1:9]))
	if seed < 0 {
		log_error(error.Error(), nil)
		return message, error
	}

	// The bug type (1, 2, or 3) is encoded as a uint8.
	message.bug = input[0]
	// The seed is encoded as a uint64
	message.seed = seed
	// The GitHub commit hash is remains as a 20 byte array
	copy(message.commit[:], input[9:29])
	// Sha256 hash of the vopr_message_byte_array is used as a unique identifier of this message
	// throughout the logs.
	message.hash = sha256.Sum256(input[:])

	return message, nil
}

// The write functionality is in its own function so it can run in its own Goroutine to allow the
// connection to be closed without delays.
func write_to_vopr_message_channel(message vopr_message, vopr_message_channel chan vopr_message) {
	vopr_message_channel <- message
	log_debug("Message has been added to channel for processing", message.hash[:])
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
	commit_string := hex.EncodeToString(message.commit[:])

	// Bugs 1 & 2 don't require a stack trace to be deduped
	if dedupe_bug_1_and_2(message) == "duplicate" {
		return
	}

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

	output.extract_stack_trace(&message)
	output.parse_stack_trace(&message)
	output.hash_stack_trace(&message)

	issue_file_name := generate_file_name(message, output.stack_trace_hash)

	// Bug 3 requires a stack trace to be deduped
	if message.bug == 3 && dedupe(issue_file_name, message.hash[:]) == "duplicate" {
		return
	}

	// Saves report to disk
	create_issue_file(issue_file_name, &output, message.hash[:])

	create_github_issue(message, &output, issue_file_name)
}

// Checks if a duplicate issue has already been submitted.
func dedupe(issue_file_name string, message_hash []byte) string {
	if _, error := os.Stat(issue_file_name); error == nil {
		log_info("Duplicate issue found", message_hash)
		return "duplicate"
	} else {
		return ""
	}
}

// Bugs 1 and 2 don't require a stack trace for deduping and so they can be deduped before the VOPR
// is run.
func dedupe_bug_1_and_2(message vopr_message) string {
	// If bug type 1 or 2 first check if file exists before parsing and hashing the stack trace
	if message.bug == 1 || message.bug == 2 {
		issue_file_name := generate_file_name(message, "")
		return dedupe(issue_file_name, message.hash[:])
	} else {
		return ""
	}
}

// Filenames are used to uniquely identify issues for deduping purposes.
// correctness: 1_seed_commithash
// liveness: 2_seed_commithash
// crash: 3_commithash_stacktracehash
func generate_file_name(message vopr_message, stack_trace_hash string) string {
	commit_string := hex.EncodeToString(message.commit[:])

	file_name := ""
	if message.bug == 1 || message.bug == 2 {
		file_name = fmt.Sprintf(
			"%s/%d_%d_%s",
			issue_directory,
			message.bug,
			message.seed,
			commit_string,
		)
	} else if message.bug == 3 {
		file_name = fmt.Sprintf(
			"%s/%d_%s_%s",
			issue_directory,
			message.bug,
			commit_string,
			stack_trace_hash,
		)
	}
	return file_name
}

// Fetch available branches from GitHub and checkout the correct commit if it exists.
func checkout_commit(commit string, message_hash []byte) error {
	// Ensures commit is all hexidecimal.
	commit_valid, error := regexp.MatchString(`^([0-9a-f]){40}$`, commit)
	if error != nil {
		error_message := fmt.Sprintf("Regex failed to run on the GitHub commit %s", error.Error())
		log_error(error_message, message_hash)
		return error
	} else if !commit_valid {
		error = fmt.Errorf("The GitHub commit contained unexpected characters")
		log_error(error.Error(), message_hash)
		return error
	}

	// Git commands need to be run with the TigerBeetle directory as their working_directory
	fetch_command := exec.Command("git", "fetch", "--all")
	fetch_command.Dir = tigerbeetle_directory
	error = fetch_command.Run()
	if error != nil {
		error_message := fmt.Sprintf("Failed to run git fetch: %s", error.Error())
		log_error(error_message, message_hash)
		return error
	}

	// Checkout the commit specified in the vopr_message
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
	vopr_completed := make(chan bool)

	vopr_path := tigerbeetle_directory + "/scripts/vopr.sh"

	// Runs in debug mode
	log_info("Running the VOPR...", message_hash)
	cmd := exec.Command(
		"/bin/bash",
		vopr_path,
		fmt.Sprintf("%d", seed),
	)
	cmd.Dir = tigerbeetle_directory
	cmd.Stderr = &limited_buffer
	// Start() runs asynchronously but is used because it allows a process to be killed.
	error := cmd.Start()

	// Wait() runs synchronously. A separate Goroutine is needed to prevent the code from blocking.
	// The VOPR might end prematurely instead if its output exceeds the maximum space.
	go func() {
		result := cmd.Wait()
		if result == nil {
			vopr_completed <- true
		} else {
			vopr_completed <- false
		}
	}()

	// The code blocks until a value is found in either the vopr_completed or size_reached channel.
	select {
	case result := <-vopr_completed:
		if result {
			log_message := fmt.Sprintf("The VOPR has completed with error: %v", error)
			log_info(log_message, message_hash)
		} else {
			log_info("The VOPR completed", message_hash)
		}
	case max_size := <-limited_buffer.size_reached:
		if max_size {
			cmd.Process.Kill()
		}
		log_info("The VOPR has completed", message_hash)
	}

	// All results are stored in the output.logs byte array
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
func create_github_issue(message vopr_message, output *vopr_output, issue_file_name string) {
	body := create_issue_markdown(message, output)
	// Removes the file path from the name.
	issue_file_name = strings.Replace(issue_file_name, issue_directory+"/", "", 1)
	issue_contents := fmt.Sprintf(
		"{ \"title\": \"%s\", \"body\": \"%s\", \"labels\":[] }",
		issue_file_name,
		body,
	)
	issue := strings.NewReader(issue_contents)
	post_request, error := http.NewRequest(
		"POST",
		repository_url,
		issue,
	)
	if error != nil {
		log_error("Failed to create the HTTP request for the GitHub API", message.hash[:])
		panic(error.Error())
	}
	post_request.Header.Set("Authorization", "token "+developer_token)

	post_response, error := http.DefaultClient.Do(post_request)
	if error != nil {
		log_error("Failed to send the HTTP request for the GitHub API", message.hash[:])
		panic(error.Error())
	}
	defer post_response.Body.Close()
	log_info("GitHub issue has been created", message.hash[:])
}

// Creates the string that forms the body of the GitHub issue.
func create_issue_markdown(message vopr_message, output *vopr_output) string {
	// Limit set here to avoid writing only a few characters for any particular section.
	const min_useful_length = 100

	// Extract the information about the conditions under which the VOPR runs TigerBeetle.
	output.extract_parameters(&message)

	length_of_stack_trace := len(output.stack_trace)
	length_of_parameters := len(output.parameters)
	length_of_logs := len(output.logs)
	remaining_space := MAX_GITHUB_ISSUE_SIZE

	stack_trace := ""
	parameters := ""
	debug_logs := ""

	// Set the stack trace string
	if length_of_stack_trace > 0 {
		if length_of_stack_trace <= MAX_GITHUB_ISSUE_SIZE {
			stack_trace = make_markdown_compatible(string(output.stack_trace[:]))
			remaining_space -= length_of_stack_trace
		} else {
			// If the stack trace is too large then just capture the beginning of it.
			stack_trace = make_markdown_compatible(string(output.stack_trace[:MAX_GITHUB_ISSUE_SIZE-4]))
			stack_trace += "..."
			remaining_space = 0
		}
	}

	// Set the parameters string
	if length_of_parameters > 0 && remaining_space >= min_useful_length {
		if length_of_parameters <= remaining_space {
			parameters = make_markdown_compatible(output.parameters[:])
			remaining_space -= length_of_parameters
		} else {
			// If the parameters section is too large then just capture the beginning of it.
			parameters = make_markdown_compatible(output.parameters[:remaining_space-4]) + "..."
			remaining_space = 0
		}
	}

	// Set the debug logs string
	if remaining_space >= min_useful_length {
		if length_of_logs < remaining_space {
			debug_logs = make_markdown_compatible(string(output.logs[:]))
		} else {
			// Get the tail of the logs
			tail_start_index := length_of_logs - remaining_space
			debug_logs = make_markdown_compatible(string(output.logs[tail_start_index:]))
		}
	}

	var bugType string
	if message.bug == 1 {
		bugType = "correctness"
	} else if message.bug == 2 {
		bugType = "liveness"
	} else if message.bug == 3 {
		bugType = "crash"
	}

	var body string
	if len(parameters) > 0 {
		body += fmt.Sprintf(
			"**Parameters:**<br>%s<br><br>",
			parameters,
		)
	}
	if len(stack_trace) > 0 {
		body += fmt.Sprintf(
			"**Stack Trace:**<br>%s<br><br>",
			stack_trace,
		)
	}
	if len(debug_logs) > 0 {
		body += fmt.Sprintf(
			"**Tail of Debug Logs:**<br>%s<br><br>",
			debug_logs,
		)
	}

	timestamp := time.Now().UTC().String()

	markdown := fmt.Sprintf(
		"**Bug Type:**<br>%s<br><br>**Seed:**<br>%d<br><br>**Commit Hash:**<br>%s<br><br>%s**Time:**<br>%s",
		bugType,
		message.seed,
		hex.EncodeToString(message.commit[:]),
		body,
		timestamp,
	)
	return markdown
}

// Escape characters that have special use in Markdown.
func make_markdown_compatible(text string) string {
	text = strings.ReplaceAll(text, "\"", "\\\"")
	text = strings.ReplaceAll(text, "<", "&lt;")
	text = strings.ReplaceAll(text, ">", "&gt;")
	text = strings.ReplaceAll(text, "\n", "<br>")
	return text
}

// Retrieve all the required environment variables up front.
func set_environment_variables() {
	var found bool
	tigerbeetle_directory, found = os.LookupEnv("TIGERBEETLE_DIRECTORY")
	if !found {
		log_error("Could not find TIGERBEETLE_DIRECTORY environmental variable", nil)
		os.Exit(1)
	} else if not_empty(&tigerbeetle_directory) {
		// Ensure there is no trailing slash
		tigerbeetle_directory = strings.TrimRight(tigerbeetle_directory, "/\\")
		log_debug("tigerbeetle_directory set as "+tigerbeetle_directory, nil)
	} else {
		log_error("TIGERBEETLE_DIRECTORY was empty", nil)
		os.Exit(1)
	}

	issue_directory, found = os.LookupEnv("ISSUE_DIRECTORY")
	if !found {
		log_error("Could not find ISSUE_DIRECTORY environmental variable", nil)
		os.Exit(1)
	} else if not_empty(&issue_directory) {
		// Ensure there is no trailing slash
		issue_directory = strings.TrimRight(issue_directory, "/\\")
		log_debug("issue_directory set as "+issue_directory, nil)
	} else {
		log_error("ISSUE_DIRECTORY was empty", nil)
		os.Exit(1)
	}

	developer_token, found = os.LookupEnv("DEVELOPER_TOKEN")
	if !found {
		log_error("Could not find DEVELOPER_TOKEN environmental variable", nil)
		os.Exit(1)
	} else if not_empty(&developer_token) {
		log_debug("developer_token has been set", nil)
	} else {
		log_error("DEVELOPER_TOKEN was empty", nil)
		os.Exit(1)
	}

	vopr_hub_address, found = os.LookupEnv("VOPR_HUB_ADDRESS")
	if !found {
		log_error("Could not find VOPR_HUB_ADDRESS environmental variable", nil)
		os.Exit(1)
	} else if not_empty(&vopr_hub_address) {
		log_debug("vopr_hub_address set as "+vopr_hub_address, nil)
	} else {
		log_error("VOPR_HUB_ADDRESS was empty", nil)
		os.Exit(1)
	}

	repository_url, found = os.LookupEnv("REPOSITORY_URL")
	if !found {
		log_error("Could not find REPOSITORY_URL environmental variable", nil)
		os.Exit(1)
	} else if not_empty(&repository_url) {
		log_debug("repository_url set as "+repository_url, nil)
	} else {
		log_error("REPOSITORY_URL was empty", nil)
		os.Exit(1)
	}
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
	if !debug_mode {
		return
	} else {
		log_message("debug: ", message, vopr_message_hash)
	}
}

func log_info(message string, vopr_message_hash []byte) {
	log_message("info:  ", message, vopr_message_hash)
}

// Formats all the log messages and adds a timestamp to them.
func log_message(log_level string, message string, vopr_message_hash []byte) {
	// Gets the current time in UTC and rounds to the nearest second.
	timestamp := time.Now().UTC().Round(time.Second).Format("2006-01-02 15:04:05.999999999")
	if vopr_message_hash != nil {
		// Only use the first 16 bytes of the message hash as the ID
		fmt.Printf(timestamp+" "+log_level+"ID:%x %s\n", vopr_message_hash[0:16], message)
	} else {
		fmt.Printf(timestamp+" "+log_level+"%s\n", message)
	}
}

func main() {
	// Determine the mode in which to run the VOPR Hub
	flag.BoolVar(&debug_mode, "debug", false, "runs with debugging logs enabled")
	flag.Parse()

	set_environment_variables()

	// This channel ensures no more than MAX_CONCURRENT_CONNECTIONS are being handled at one time.
	track_connections := make(chan bool, MAX_CONCURRENT_CONNECTIONS)

	// The channel will receieve fixed-size byte arrays from a VOPR.
	vopr_message_channel := make(chan vopr_message, MAX_QUEUING_MESSAGES)

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
			connection.Close();
			continue;
		}

		select {
		case track_connections <- true:
			go handle_connection(track_connections, connection, vopr_message_channel)
		default:
			log_info("Connection closed because there are currently too many open connections", nil)
			connection.Close()
		}
	}
}
