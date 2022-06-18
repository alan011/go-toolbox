package execmd

import (
	"errors"
	"io"
	"os/exec"
)


type Result struct {
	ExitCode 	int 	`json:"exit_code"`
	Stdout		string	`json:"stdout"`
	Stderr 		string	`json:"stderr"`
}

func Run(strCmd string, args ...string) (*Result, error) {
	/*
	如果不提供args，则会被认为是一个onelineCmd，将用'bash -c'来执行。

	提供了args，则strCmd被认为是一个program，将按照exec原生的方式执行。
	*/

	res := Result{ExitCode: -1}

	// Init
	var cmd *exec.Cmd
	if len(args) == 0 {
		cmd = exec.Command("/bin/bash", "-c", strCmd)
	} else {
		cmd = exec.Command(strCmd, args...)
	}
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	// Start to execute. Do record.
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	stdoutBytes, err := io.ReadAll(stdout)
	if err != nil {
		return nil, err
	}
	stderrBytes, err := io.ReadAll(stderr)
	if err != nil {
		return nil, err
	}
	
	// Wait until cmd executing complete.
	if err := cmd.Wait(); err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if ok {
			res.ExitCode = exitErr.ProcessState.ExitCode()
			if res.ExitCode == -1 {
				return nil, errors.New("ERROR: Cmd process was not started successfully or has been killed!")
			}
		} else {
			return nil, err
		}
	} else {
		res.ExitCode = 0
	}

	// Store results.
	res.Stdout = string(stdoutBytes)
	res.Stderr = string(stderrBytes)

	return &res, nil
}
