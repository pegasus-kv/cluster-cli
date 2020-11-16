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
	"time"
)

// TODO(wutao): refactor to `waitFor(checker func() (bool, error), timeout time.Duration)`
func waitFor(checker func() (bool, error), interval time.Duration, timeout int) (bool, error) {
	i := 0
	for {
		res, err := checker()
		if err != nil {
			return false, err
		}
		if !res {
			i++
			if timeout != 0 && i > timeout {
				return false, nil
			}
		} else {
			return true, nil
		}
		time.Sleep(interval)
	}
}

func newCommandError(msg string, out []byte) error {
	return errors.New(msg + ". Output:\n" + string(out))
}
