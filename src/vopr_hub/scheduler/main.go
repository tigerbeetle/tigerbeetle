/*
The scheduler checks out the correct commit on each VOPR to test the latest code on main and
on pull requests with the `vopr` label.
Note: The scheduler must run in a directory which is separate from the VOPRs it manages to
prevent it from checking out a commit that could change the organizer itself.
To run the scheduler, four environmental variables are required:
1. TIGERBEETLE_DIRECTORY the location of the VOPR's tigerbeetle directory
2. REPOSITORY_URL to access TigerBeetle's GitHub repository
3. DEVELOPER_TOKEN required for making GitHub API calls
4. NUM_VOPRS specifies the number of VOPRs the system is running
5. CURRENT_VOPR specifies which VOPR is currently starting up and requiring updated Git commit info
*/

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const usageFmt = `usage: %s [flags]

environment:
  TIGERBEETLE_DIRECTORY  the location of the VOPR's tigerbeetle directory
  REPOSITORY_URL         TigerBeetle's GitHub repository URL
  DEVELOPER_TOKEN        required for making GitHub API calls
  NUM_VOPRS              the number of VOPRs the system is running
  CURRENT_VOPR           the number of the VOPR which is currently starting up (counting from 0)

flags:
  -debug  enabled debug logging
`

var (
	debug_mode            bool
	developer_token       string
	tigerbeetle_directory string
	repository_url        string
	num_voprs             int
	current_vopr          int
)

type Label struct {
	Name string `json:"name"`
}

type Head struct {
	Label string `json:"label"`
}

type Issue struct {
	Labels []Label `json:"labels"`
	Head   Head    `json:"head"`
}

type Commit struct {
	Sha string `json:"sha"`
}

func usage() {
	fmt.Fprintf(os.Stderr, usageFmt, os.Args[0])
	os.Exit(1)
}

func load_environment_variable(name string) string {
	val, _ := os.LookupEnv(name)
	if val == "" {
		log_error(fmt.Sprintf("env %s not set", name))
		usage()
		os.Exit(1)
	} else if name == "DEVELOPER_TOKEN" {
		log_debug(fmt.Sprintf("env %s=*****", name))
	} else {
		log_debug(fmt.Sprintf("env %s=%q", name, val))
	}
	return val
}

// Checks out the appropriate commit for the specified tigerbeetle directory if it exists.
func checkout_commit(commit string, tigerbeetle_directory string) error {
	// Git commands need to be run with the particular TigerBeetle directory as their
	// working_directory.
	fetch_command := exec.Command("git", "fetch", "--all")
	fetch_command.Dir = tigerbeetle_directory
	error := fetch_command.Run()
	if error != nil {
		error_message := fmt.Sprintf("Failed to run git fetch: %s", error.Error())
		log_error(error_message)
		return error
	}

	// Ensures commit is all hexadecimal.
	commit_valid, error := regexp.MatchString(`^([0-9a-f]){40}$`, commit)
	if error != nil {
		panic(error.Error())
	} else if !commit_valid {
		error = fmt.Errorf("The GitHub commit contained unexpected characters")
		log_error(error.Error())
		return error
	}

	// Checkout the commit.
	checkout_command := exec.Command("git", "checkout", commit)
	checkout_command.Dir = tigerbeetle_directory
	error = checkout_command.Run()
	if error != nil {
		error_message := fmt.Sprintf("Failed to run git checkout: %s", error.Error())
		log_error(error_message)
		return error
	}

	// Inspect the git logs.
	log_command := exec.Command("git", "log", "-1")
	log_command.Dir = tigerbeetle_directory
	log_output := make([]byte, 47)
	log_output, error = log_command.Output()
	if error != nil {
		error_message := fmt.Sprintf("Failed to run git log: %s", error.Error())
		log_error(error_message)
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
		log_error(error_message)
		return error
	}

	if !checkout_successful {
		error = fmt.Errorf("Checkout failed")
		return error
	}

	return nil
}

func get_github_content(path string, result interface{}) {
	for {
		get_request, err := http.NewRequest("GET", path, nil)
		if err != nil {
			log_error("unable to create get request")
			panic(err.Error())
		}

		get_request.Header.Set("Authorization", "token "+developer_token)
		res, err := http.DefaultClient.Do(get_request)
		if err != nil {
			log_error("Failed to send the HTTP request for the GitHub API")
			panic(err.Error())
		}

		body, err := io.ReadAll(res.Body)
		res.Body.Close()

		// Handle rate limiting:
		// https://docs.github.com/en/rest/overview/resources-in-the-rest-api?apiVersion=2022-11-28#exceeding-the-rate-limit
		if res.StatusCode == 403 && res.Header.Get("x-ratelimit-limit") == "0" {
			reset_at := res.Header.Get("x-ratelimit-reset")
			expires, err := strconv.ParseInt(reset_at, 10, 64)
			if err != nil {
				log_error(fmt.Sprintf("Failed to parse rate limit reset %s", reset_at))
				panic(err.Error())
			}
			
			deadline := time.Unix(expires, 0)
			time.Sleep(deadline.Sub(time.Now()))
			continue
		}

		if res.StatusCode > 299 {
			log_error(
				fmt.Sprintf(
					"Response failed with status code: %d and\nbody: %s\n",
					res.StatusCode,
					body,
				),
			)
			panic(err.Error())
		}

		if err != nil {
			log_error("unable to receive a response from GitHub")
			panic(err.Error())
		}

		err = json.Unmarshal(body, result)
		if err != nil {
			log_error("unable to unmarshall json")
			panic(err.Error())
		}

		return
	}	
}

func get_pull_requests(num_posts int, page_number int) []Issue {
	pull_requests := []Issue{}
	get_github_content(
		fmt.Sprintf("%s/pulls?per_page=%d&page=%d", repository_url, num_posts, page_number),
		&pull_requests,
	)

	return pull_requests
}

func get_commits(branch_name string) string {
	commits := []Commit{}
	get_github_content(
		fmt.Sprintf("%s/commits?per_page=1&sha=%s", repository_url, branch_name),
		&commits,
	)

	if len(commits) > 0 && len(commits[0].Sha) > 0 {
		return commits[0].Sha
	} else {
		error := fmt.Errorf("unable to access commit associated with branch %s", branch_name)
		panic(error.Error())
	}
}

func get_commit_hashes() []string {
	const num_posts int = 30 // This is the GitHub API default.
	var pull_requests []Issue
	var vopr_commits []string

	page_number := 1

	// Require at most (num_voprs-1) pull requests.
	// The first VOPR always runs on main's latest commit.
	for len(vopr_commits) < num_voprs-1 {
		// Pull requests will be ordered newest to oldest by default.
		pull_requests = get_pull_requests(num_posts, page_number)

		for _, element := range pull_requests {
			for _, label := range element.Labels {
				if label.Name == "vopr" {
					// Branches are returned in the format owner:branch_name.
					_, branch_name, found := strings.Cut(element.Head.Label, ":")
					if found && branch_name != "" {
						commit := get_commits(branch_name)
						vopr_commits = append(vopr_commits, commit)
					}
					break
				}
			}

			if len(vopr_commits) == num_voprs-1 {
				break
			}
		}
		// Exit the loop if there are no more pages of pull requests to be fetched from GitHub.
		if len(pull_requests) < num_posts {
			break
		}
		page_number++
	}

	return vopr_commits
}

func get_vopr_assignments(vopr_commits []string) []string {
	var num_pull_requests = len(vopr_commits)
	var vopr_assignments []string
	main_commit := get_commits("main")

	if num_pull_requests > 0 {
		// The first VOPR always runs main
		vopr_assignments = append(vopr_assignments, main_commit)

		// This calculates how many times each PR branch will be assigned to a VOPR.
		var repeats = int((num_voprs - 1) / num_pull_requests)
		// This calculates how many branches will have an additional assignment.
		var remainders = (num_voprs - 1) % num_pull_requests
		i := 1
		commit_index := 0
		for i < num_voprs {
			for j := 0; j < repeats; j++ {
				vopr_assignments = append(
					vopr_assignments,
					fmt.Sprintf("%s", vopr_commits[commit_index]),
				)
				i++
			}
			if remainders > 0 {
				vopr_assignments = append(
					vopr_assignments,
					fmt.Sprintf("%s", vopr_commits[commit_index]),
				)
				remainders--
				i++
			}
			commit_index++
		}
	} else {
		i := 0
		for i < num_voprs {
			vopr_assignments = append(vopr_assignments, main_commit)
			i++
		}
	}
	return vopr_assignments
}

func log_error(message string) {
	log_message("error: ", message)
}

func log_debug(message string) {
	if debug_mode {
		log_message("debug: ", message)
	}
}

// Formats all the log messages and adds a timestamp to them.
func log_message(log_level string, message string) {
	// Gets the current time in UTC and rounds to the nearest second.
	timestamp := time.Now().UTC().Format(time.RFC3339)
	fmt.Printf("%s %s%s\n", timestamp, log_level, message)
}

func main() {
	// Determine the mode in which to run the VOPR Hub.
	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flags.BoolVar(&debug_mode, "debug", false, "runs with debugging logs enabled")
	if err := flags.Parse(os.Args[1:]); err != nil {
		usage()
	}

	tigerbeetle_directory = load_environment_variable("TIGERBEETLE_DIRECTORY")
	repository_url = load_environment_variable("REPOSITORY_URL")
	developer_token = load_environment_variable("DEVELOPER_TOKEN")
	num_voprs_str := load_environment_variable("NUM_VOPRS")
	// string to int
	var err error
	num_voprs, err = strconv.Atoi(num_voprs_str)
	if err != nil {
		log_error("unable to convert num_voprs to a an integer value")
		panic(err.Error())
	} else if num_voprs <= 0 {
		log_error("NUM_VOPRS must be an integer greater than 0")
		os.Exit(1)
	}
	current_vopr_str := load_environment_variable("CURRENT_VOPR")
	// string to int
	current_vopr, err = strconv.Atoi(current_vopr_str)
	if err != nil {
		log_error("unable to convert current_vopr to a an integer value")
		panic(err.Error())
	} else if current_vopr < 0 {
		log_error("CURRENT_VOPR must be a positive integer")
		os.Exit(1)
	}

	// Gets commit hashes for main and up to (NUM_VOPRS -1) PR branches that have the `vopr` label.
	vopr_commits := get_commit_hashes()

	// Assigns one commit for each VOPR to run on.
	vopr_assignments := get_vopr_assignments(vopr_commits)

	if current_vopr < len(vopr_assignments) && current_vopr >= 0 {
		error := checkout_commit(
			vopr_assignments[current_vopr],
			fmt.Sprintf("%s%d", tigerbeetle_directory, current_vopr),
		)
		if error == nil {
			log_debug("Successfully checked out commit " + vopr_assignments[current_vopr])
		} else {
			error_message := fmt.Sprintf(
				"Failed to checkout commit %s: %s",
				vopr_assignments[current_vopr],
				error.Error(),
			)
			log_error(error_message)
			panic(error.Error())
		}
	}
}
