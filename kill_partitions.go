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
	"errors"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
)

func killPartitions(node *util.PegasusNode, partitions []*base.Gpid) error {
	for _, p := range partitions {
		err := client.CallCmd(node, "replica.kill_partition",
			[]string{fmt.Sprintf("%d.%d", p.Appid, p.PartitionIndex)}).Error()
		if err != nil {
			return err
		}
	}
	return nil
}

func killAndWaitPartitions(node *util.PegasusNode, partitions []*base.Gpid) error {
	sleptSecs := 0

	// Wait until the node is confirmed to have no replica.
	for {
		if sleptSecs >= 28 {
			return errors.New("DowngradeNode timeout")
		}
		if sleptSecs%10 == 0 {
			err := killPartitions(node, partitions)
			if err != nil {
				return err
			}
		}

		res := client.CallCmd(node, "perf-counters", []string{".*replica(Count)"})
		if res.Failed() {
			return res.Error()
		}
		count := 0
		for _, counter := range counters {
			count += int(counter.Value)
		}

		time.Sleep(time.Second)
		sleptSecs++
	}
}
