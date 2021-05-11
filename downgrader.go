package pegasus

import (
	"github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
	"github.com/pegasus-kv/cluster-cli/deployment"
	metaApi "github.com/pegasus-kv/cluster-cli/meta"
)

// Downgrader safely and gracefully downgrades all replicas on this node.
//
// NOTE: It adds a level of abstraction between rolling-update/remove_node and MetaServer,
// so that we can mock this step, without mocking Meta.
type Downgrader interface {
	Downgrade(node *util.PegasusNode) error
}

type downgrader struct {
	meta   metaApi.Meta
	deploy deployment.Deployment
}

func newDowngrader(m metaApi.Meta, deploy deployment.Deployment) Downgrader {
	return &downgrader{meta: m, deploy: deploy}
}

func (d *downgrader) Downgrade(node *util.PegasusNode) error {
	// Safely downgrades replicas from node, as no primary was directly effected.
	if err := d.meta.MigratePrimariesOut(node); err != nil {
		return err
	}
	downgradedParts, err := d.meta.DowngradeNodeWithDetails(node)
	if err != nil {
		return err
	}
	if err := killAndWaitPartitions(node, downgradedParts); err != nil {
		return err
	}

	// flush log to stderr or log-file
	if err := client.CallCmd(node, "flush-log", []string{}).Error(); err != nil {
		return err
	}

	return nil
}
