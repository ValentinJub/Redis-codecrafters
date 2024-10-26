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
	{
		description:       "Set command: NX a key that already exists",
		commandSet:        "redis-cli",
		argsSet:           []string{"SET", "transport", "Car", "NX"}, // set if key does not exist
		expectedSetOutput: "\n",
		commandGet:        "redis-cli",
		argsGet:           []string{"GET", "transport"},
		expectedGetOutput: "Car\n",
		expiry:            0,
		sleep:             0,
	},
}

func StartMasterTestServer() *MasterServer {
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
