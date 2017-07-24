package command

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/codegangsta/cli"

	"github.com/ksarch-saas/cc/cli/context"
	"github.com/ksarch-saas/cc/controller/command"
	"github.com/ksarch-saas/cc/frontend/api"
	"github.com/ksarch-saas/cc/redis"
	"github.com/ksarch-saas/cc/topo"
	"github.com/ksarch-saas/cc/utils"
)

var GenerateRdbPathCommand = cli.Command{
	Name:   "genrdb",
	Usage:  "genrdb -r=[bj/nj/gz] -c=[master/oneslave/allslave/all] -i=[60] -s=[ture/false] ",
	Action: genRdbAction,
	Flags: []cli.Flag{
		cli.StringFlag{"r,region", "", "region to gernate rdb"},
		cli.StringFlag{"c,dump candidates", "oneslave", "master/oneslave/allslave/all"},
		cli.BoolFlag{"s,save", "really send bgsave command"},
		cli.IntFlag{"i,interval", 60, "send bgsave to instance interval"},
	},
	Description: `
	generate rdb files for each replica set
	`,
}

func BgSaveRedis(node *topo.Node, really_save bool, interval int) error {
	if really_save {
		response, err := redis.RedisCli(node.Addr(), "BGSAVE")
		res := response.(string)
		if err != nil || res != "Background saving started" {
			err_info := fmt.Sprintf("bgsave node:%s fail:%s, response:%s\n", node.Addr(), err.Error(), res)
			return errors.New(err_info)
		}
		time.Sleep(time.Duration(interval) * time.Second)
	}

	unitId := strings.Split(node.Tag, ".")[1]
	fmt.Printf("save node:%s(%s,%s) to ftp://%s/home/matrix/containers/%s.redis3db-%s.osp.%s/home/work/data/dump.rdb\n",
		node.Id, node.Role, node.Addr(), node.Ip, unitId, context.GetAppName(), node.Zone)
	return nil
}

func genRdbAction(c *cli.Context) {
	region := c.String("r")

	candidates := c.String("c")

	if candidates != "master" && candidates != "oneslave" && candidates != "allslave" && candidates != "all" {
		fmt.Printf("invalid candidates\n")
		return
	}

	interval := c.Int("i")

	really_save := c.Bool("s")

	addr := context.GetLeaderAddr()
	url := "http://" + addr + api.FetchReplicaSetsPath

	resp, err := utils.HttpGet(url, nil, 5*time.Second)
	if err != nil {
		fmt.Println(err)
		return
	}

	var rss command.FetchReplicaSetsResult
	err = utils.InterfaceToStruct(resp.Body, &rss)
	if err != nil {
		fmt.Println(err)
		return
	}
	sort.Sort(topo.ByMasterId(rss.ReplicaSets))
	sort.Sort(topo.ByNodeState(rss.ReplicaSets))

	for _, rs := range rss.ReplicaSets {
		// process master
		if candidates == "all" || candidates == "master" {
			err = BgSaveRedis(rs.Master, really_save, interval)
			if err != nil {
				fmt.Println(err)
				return
			}
		}
		// process slaves
		if candidates != "master" {
			for _, n := range rs.Slaves {
				if region == "" || region == n.Region {
					err = BgSaveRedis(n, really_save, interval)
					if err != nil {
						fmt.Println(err)
						return
					}
					if candidates == "oneslave" {
						break
					}
				}
			}
		}
	}

	fmt.Println("ok, all finished!\n")
	return
}
