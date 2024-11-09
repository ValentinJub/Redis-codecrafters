package server

import (
	"fmt"
	"os/exec"
	"testing"
	"time"
)

const APP = "../your_program.sh"

var TestCases = []struct {
	description    string
	command        string
	args           []string
	expectedOutput string
}{
	{
		description:    "Ping command",
		command:        "redis-cli",
		args:           []string{"PING"},
		expectedOutput: "PONG\n",
	},
	{
		description:    "Ping with arg command",
		command:        "redis-cli",
		args:           []string{"PING", "Hellou you silly pirate"},
		expectedOutput: "Hellou you silly pirate\n",
	},
	{
		description:    "Echo command",
		command:        "redis-cli",
		args:           []string{"ECHO", "'Hello World'"},
		expectedOutput: "'Hello World'\n",
	},
	{
		description:    "Set command",
		command:        "redis-cli",
		args:           []string{"SET", "name", "John"},
		expectedOutput: "OK\n",
	},
	{
		description:    "Get command",
		command:        "redis-cli",
		args:           []string{"GET", "name"},
		expectedOutput: "John\n",
	},
	{
		description:    "Get command",
		command:        "redis-cli",
		args:           []string{"GET", "unknown"},
		expectedOutput: "\n",
	},
}

var TestSetCases = []struct {
	description       string
	commandSet        string
	argsSet           []string
	expectedSetOutput string
	commandGet        string
	argsGet           []string
	expectedGetOutput string
	expiry            int64
	sleep             int64
}{
	{
		description:       "Set command: 1 second expiry with 0.5 second sleep",
		commandSet:        "redis-cli",
		argsSet:           []string{"SET", "name", "John", "EX", "1"}, // One second expiry
		expectedSetOutput: "OK\n",
		commandGet:        "redis-cli",
		argsGet:           []string{"GET", "name"},
		expectedGetOutput: "John\n",
		expiry:            1000,
		sleep:             500,
	},
	{
		description:       "Set command: 1 second expiry with 1.1 second sleep",
		commandSet:        "redis-cli",
		argsSet:           []string{"SET", "name", "John", "EX", "1"}, // One second expiry
		expectedSetOutput: "OK\n",
		commandGet:        "redis-cli",
		argsGet:           []string{"GET", "name"},
		expectedGetOutput: "\n",
		expiry:            1000,
		sleep:             1001,
	},
	{
		description:       "Set command: 1000 ms expiry with 900 ms sleep",
		commandSet:        "redis-cli",
		argsSet:           []string{"SET", "color", "Purple", "PX", "1000"}, // One second expiry
		expectedSetOutput: "OK\n",
		commandGet:        "redis-cli",
		argsGet:           []string{"GET", "color"},
		expectedGetOutput: "Purple\n",
		expiry:            1000,
		sleep:             900,
	},
	{
		description:       "Set command: 1000 ms expiry with 1001 ms sleep",
		commandSet:        "redis-cli",
		argsSet:           []string{"SET", "color", "Purple", "PX", "1000"}, // One second expiry
		expectedSetOutput: "OK\n",
		commandGet:        "redis-cli",
		argsGet:           []string{"GET", "color"},
		expectedGetOutput: "\n",
		expiry:            1000,
		sleep:             1001,
	},
	{
		description:       "Set command: NX a key that does not exist",
		commandSet:        "redis-cli",
		argsSet:           []string{"SET", "transport", "Car", "NX"}, // only set if key does not exist
		expectedSetOutput: "OK\n",
		commandGet:        "redis-cli",
		argsGet:           []string{"GET", "transport"},
		expectedGetOutput: "Car\n",
		expiry:            0,
		sleep:             0,
	},
}

var DelTestCases = []struct {
	description    string
	commands       [][]string
	expectedOutput []string
}{
	{
		description: "DEL command: delete one key",
		commands: [][]string{
			{"SET", "name", "John"},
			{"DEL", "name"},
			{"GET", "name"},
		},
		expectedOutput: []string{"OK\n", "1\n", "\n"},
	},
	{
		description: "DEL command: delete multiple keys",
		commands: [][]string{
			{"SET", "name", "John"},
			{"SET", "color", "Purple"},
			{"DEL", "name", "color"},
			{"GET", "name"},
			{"GET", "color"},
		},
		expectedOutput: []string{"OK\n", "OK\n", "2\n", "\n", "\n"},
	},
	{
		description: "DEL command: delete non-existent key",
		commands: [][]string{
			{"DEL", "name"},
			{"GET", "name"},
		},
		expectedOutput: []string{"0\n", "\n"},
	},
}

var CopyTestCases = []struct {
	description    string
	commands       [][]string
	expectedOutput []string
}{
	{
		description: "COPY command: copy one key",
		commands: [][]string{
			{"SET", "name", "John"},
			{"COPY", "name", "new_name"},
			{"GET", "new_name"},
		},
		expectedOutput: []string{"OK\n", "1\n", "John\n"},
	},
	{
		description: "COPY command: copy on an existing key without the REPLACE option",
		commands: [][]string{
			{"SET", "name", "John"},
			{"SET", "new_name", "Jane"},
			{"COPY", "name", "new_name"},
			{"GET", "new_name"},
		},
		expectedOutput: []string{"OK\n", "OK\n", "0\n", "Jane\n"},
	},
	{
		description: "COPY command: copy on an existing key with the REPLACE option",
		commands: [][]string{
			{"SET", "name", "Paul"},
			{"SET", "new_name", "Jane"},
			{"COPY", "name", "new_name", "REPLACE"},
			{"GET", "new_name"},
		},
		expectedOutput: []string{"OK\n", "OK\n", "1\n", "Paul\n"},
	},
}

var ExistsTestCases = []struct {
	description    string
	commands       [][]string
	expectedOutput []string
}{
	{
		description: "EXISTS command: check if key exists",
		commands: [][]string{
			{"SET", "name", "John"},
			{"EXISTS", "name"},
		},
		expectedOutput: []string{"OK\n", "1\n"},
	},
	{
		description: "EXISTS command: check if key does not exist",
		commands: [][]string{
			{"EXISTS", "balls"},
		},
		expectedOutput: []string{"0\n"},
	},
	{
		description: "EXISTS command: check if multiple keys exist",
		commands: [][]string{
			{"SET", "name", "Poulo"},
			{"EXISTS", "name"},
			{"EXISTS", "hell"},
			{"SET", "color", "Purple"},
			{"EXISTS", "name", "color"},
		},
		expectedOutput: []string{"OK\n", "1\n", "0\n", "OK\n", "2\n"},
	},
}

func StartMasterTestServer() RedisServer {
	server := NewMasterServer(map[string]string{})
	server.Init()
	return server
}

func runCommand(command string, args ...string) (string, error) {
	cmd := exec.Command(command, args...)
	output, err := cmd.Output()
	return string(output), err
}

func TestCommands(t *testing.T) {
	server := StartMasterTestServer()
	go server.Listen()
	for _, tc := range TestCases {
		t.Run(tc.description, func(t *testing.T) {
			output, err := runCommand(tc.command, tc.args...)
			if err != nil {
				fmt.Printf("Output: %s\n", output)
				t.Fatalf("error while running the test: %s", err)
			}
			if output != tc.expectedOutput {
				t.Fatalf("expected output: %s, got: %s", tc.expectedOutput, output)
			}
		})
	}
}

func TestSetCommand(t *testing.T) {
	// server := StartMasterTestServer()
	// go server.Listen()
	for _, tc := range TestSetCases {
		t.Run(tc.description, func(t *testing.T) {
			outSet, err := runCommand(tc.commandSet, tc.argsSet...)
			if err != nil {
				t.Fatalf("error while running the test: %s", err)
			} else if outSet != tc.expectedSetOutput {
				t.Fatalf("expected output: %s, got: %s", tc.expectedSetOutput, outSet)
			}

			// Sleep for the required time
			fmt.Printf("sleep for: %d ms\n", tc.sleep)
			time.Sleep(time.Millisecond * time.Duration(tc.sleep))

			outGet, err := runCommand(tc.commandGet, tc.argsGet...)
			if err != nil {
				t.Fatalf("error while running the test: %s", err)
			}
			if outGet != tc.expectedGetOutput {
				t.Fatalf("expected output: %s, got: %s", tc.expectedGetOutput, outGet)
			}
		})
	}
}

func TestDelCommand(t *testing.T) {
	// server := StartMasterTestServer()
	// go server.Listen()
	for _, tc := range DelTestCases {
		t.Run(tc.description, func(t *testing.T) {
			for i, commands := range tc.commands {
				out, err := runCommand("redis-cli", commands...)
				if err != nil {
					t.Fatalf("error while running the test: %s", err)
				}
				if out != tc.expectedOutput[i] {
					t.Fatalf("expected output: %s, got: %s", tc.expectedOutput[i], out)
				}
			}
		})
	}
}

func TestCopyCommand(t *testing.T) {
	// server := StartMasterTestServer()
	// go server.Listen()
	for _, tc := range CopyTestCases {
		t.Run(tc.description, func(t *testing.T) {
			for i, commands := range tc.commands {
				out, err := runCommand("redis-cli", commands...)
				if err != nil {
					t.Fatalf("error while running the test: %s", err)
				}
				if out != tc.expectedOutput[i] {
					t.Fatalf("expected output: %s, got: %s", tc.expectedOutput[i], out)
				}
			}
		})
	}
}

func TestExistsCommand(t *testing.T) {
	// server := StartMasterTestServer()
	// go server.Listen()
	for _, tc := range ExistsTestCases {
		t.Run(tc.description, func(t *testing.T) {
			for i, commands := range tc.commands {
				out, err := runCommand("redis-cli", commands...)
				if err != nil {
					t.Fatalf("error while running the test: %s", err)
				}
				if out != tc.expectedOutput[i] {
					t.Fatalf("expected output: %s, got: %s", tc.expectedOutput[i], out)
				}
			}
		})
	}
}
