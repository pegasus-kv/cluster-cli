/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pegasus

import (
	"bufio"
	"bytes"
	"io"
	"os/exec"
	"strings"
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

	if _, err := io.WriteString(stdin, input+"\n"); err != nil {
		return nil, err
	}
	return cmd, nil
}

func runSh(arg ...string) *exec.Cmd {
	cmd := exec.Command("./run.sh", arg...)
	cmd.Dir = shellDir
	return cmd
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
			break
		}
	}
	return out, scanner.Err()
}

func checkOutputContainsOnce(cmd *exec.Cmd, stderr bool, substr string) (bool, []byte, error) {
	count := 0
	out, err := checkOutput(cmd, stderr, func(line string) bool {
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
