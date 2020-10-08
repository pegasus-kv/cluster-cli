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
	"fmt"
	"strconv"
	"strings"
	"time"
)

func killPartitions(gpids []string, node Node, metaList string) error {
	fmt.Println("Send kill_partition commands to node...")
	for _, gpid := range gpids {
		cmd, err := runShellInput("remote_command -l "+node.IPPort+" replica.kill_partition "+gpid, metaList)
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return err
		}
	}
	fmt.Println("Sent to " + strconv.Itoa(len(gpids)) + " partitions")
	return nil
}

func setMetaLevel(level string, metaList string) error {
	fmt.Println("Set meta level to " + level + "...")
	cmd, err := runShellInput("set_meta_level "+level, metaList)
	if err != nil {
		return err
	}
	ok, out, err := checkOutputContainsOnce(cmd, false, "control meta level ok")
	if err != nil {
		return err
	}
	if !ok {
		return NewDeployError("set meta level to "+level+" failed", out)
	}
	return nil
}

func setRemoteCommand(pmeta string, attr string, value string, metaList string, pattern string) error {
	fmt.Println("Set " + attr + " to " + value + "...")
	cmd, err := runShellInput(fmt.Sprintf("remote_command -l %s %s %s", pmeta, attr, value), metaList)
	if err != nil {
		return err
	}
	ok, out, err := checkOutputContainsOnce(cmd, true, pattern)
	if err != nil {
		return err
	}
	if !ok {
		return NewDeployError("set "+attr+" to "+value+" failed", out)
	}
	return nil
}

func waitForHealthy(metaList string) error {
	fmt.Println("Wait cluster to become healthy...")
	_, err := waitFor(func() (bool, error) {
		cmd, err := runShellInput("ls -d", metaList)
		if err != nil {
			return false, err
		}
		flag := false
		count := 0
		if _, err := checkOutput(cmd, false, func(line string) bool {
			if flag {
				ss := strings.Fields(line)
				if len(ss) < 7 {
					flag = false
				} else if ss[2] != ss[3] {
					s5, err := strconv.Atoi(ss[4])
					if err != nil {
						return false
					}
					s6, err := strconv.Atoi(ss[5])
					if err != nil {
						return false
					}
					count += s5 + s6
				}
			}
			if strings.Contains(line, " fully_healthy ") {
				flag = true
			}
			return false
		}); err != nil {
			return false, err
		}
		if count == 0 {
			fmt.Println("Cluster becomes healthy")
			return true, nil
		}
		fmt.Println("Cluster not healthy, unhealthy_partition_count = " + strconv.Itoa(count))
		return false, nil
	}, time.Duration(10)*time.Second, 0)
	return err
}
