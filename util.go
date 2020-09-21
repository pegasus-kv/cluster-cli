package pegasus

import (
	"time"
)

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
