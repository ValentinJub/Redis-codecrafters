package server

import (
	"fmt"
	"os/exec"
	"testing"
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
		expectedOutput: "(nil)\n",
	},
}

func StartMasterTestServer() *MasterServer {
	server := NewMasterServer("127.0.0.1", "6379")
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
