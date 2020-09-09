package pegasus

import (
	"bufio"
	"bytes"
	"io"
	"os/exec"
	"strings"
	"time"
)

var shellDir string

func SetShellDir(dir string) {
	shellDir = dir
}

func runShellInput(input string, arg ...string) (*exec.Cmd, error) {
	cmd := exec.Command("./run.sh", append([]string{"shell", "--cluster"}, arg...)...)
	cmd.Dir = shellDir
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	defer stdin.Close()

	io.WriteString(stdin, input+"\n")
	return cmd, nil
}

func runSh(arg ...string) *exec.Cmd {
	cmd := exec.Command("./run.sh", arg...)
	cmd.Dir = shellDir
	return cmd
}

func startRunShellInput(input string, arg ...string) error {
	cmd, err := runShellInput(input, arg...)
	if err != nil {
		return err
	}
	return cmd.Run()
}

func checkOutput(cmd *exec.Cmd, stderr bool, checker func(line string) bool) ([]byte, error) {
	var (
		out []byte
		err error
	)
	if stderr {
		out, err = cmd.CombinedOutput()
	} else {
		out, err = cmd.Output()
	}
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(out)
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		if fin := checker(scanner.Text()); fin {
			break;
		}
	}
	return out, scanner.Err()
}

func checkOutputContainsOnce(cmd *exec.Cmd, substr string) (bool, []byte, error) {
	count := 0
	out, err := checkOutput(cmd, false, func(line string) bool {
		if strings.Contains(line, substr) {
			count++
			return count > 1
		}
		return false
	})
	if err != nil {
		return false, out, err
	}
	return count == 1, out, nil
}

func waitFor(fetchValue func() (interface{}, error), pred func(interface{}) bool, interval time.Duration, timeout int) (bool, error)  {
	i := 0
	for {
		val, err := fetchValue()
		if err != nil {
			return false, err
		}
		if !pred(val) {
			i += 1;
			if timeout != 0 && i > timeout {
				return false, nil
			}
		} else {
			return true, nil
		}
		time.Sleep(interval)
	}
}
