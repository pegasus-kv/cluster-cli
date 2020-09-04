package pegasus

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

type NodeList []string

func (l *NodeList) String() string {
	return strings.Join(*l, ",")
}

func (l *NodeList) Set(val string) error {
	*l = append(*l, val)
	return nil
}

var ShellDir string

func init() {
	ShellDir = os.Getenv("PEGASUS_SHELL_PATH")
	if ShellDir == "" {
		panic("env PEGASUS_SHELL_PATH not provided")
	}
}

func runShellInput(input string, arg ...string) (*exec.Cmd, error) {
	cmd := exec.Command(path.Join(ShellDir, "run.sh"), append([]string{"shell", "--cluster"}, arg...)...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	defer stdin.Close()

	io.WriteString(stdin, input+"\n")
	return cmd, nil
}

func runSh(arg ...string) *exec.Cmd {
	return exec.Command(path.Join(ShellDir, "run.sh"), arg...)
}

func startRunShellInput(input string, arg ...string) error {
	cmd, err := runShellInput(input, arg...)
	if err != nil {
		return err
	}
	return cmd.Start()
}

func checkOutput(cmd *exec.Cmd, checker func(line string) bool) error {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	err = cmd.Start()
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if fin := checker(scanner.Text()); fin {
			break;
		}
	}
	return scanner.Err()
}

func checkOutputContainsOnce(cmd *exec.Cmd, substr string) (bool, error) {
	count := 0
	if err := checkOutput(cmd, func(line string) bool {
		if strings.Contains(line, substr) {
			count++
			return count > 1
		}
		return false
	}); err != nil {
		return false, err
	}
	return count == 1, nil
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
	return true, nil
}
