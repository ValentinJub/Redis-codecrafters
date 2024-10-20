package server

import (
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
}

func StartServer() *exec.Cmd {
	cmd := exec.Command("sh", APP)
	err := cmd.Start()
	if err != nil {
		panic(err)
	}
	return cmd
}

func runCommand(command string, args ...string) (string, error) {
	cmd := exec.Command(command, args...)
	output, err := cmd.Output()
	return string(output), err
}

func TestPing(t *testing.T) {
	serverCmd := StartServer()
	defer serverCmd.Process.Kill() // Ensure the server is stopped after the test

	for _, tc := range TestCases {
		t.Run(tc.description, func(t *testing.T) {
			output, err := runCommand(tc.command, tc.args...)
			if err != nil {
				t.Fatalf("error while running the test: %s", err)
			}
			if output != tc.expectedOutput {
				t.Fatalf("expected output: %s, got: %s", tc.expectedOutput, output)
			}
		})
	}
}
