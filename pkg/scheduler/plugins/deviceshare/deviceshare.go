/*
Copyright 2024 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package deviceshare

import (
	"context"
	"fmt"
	"math"
	"reflect"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/api/devices"
	"volcano.sh/volcano/pkg/scheduler/api/devices/ascend/ascend310p/vnpu"
	"volcano.sh/volcano/pkg/scheduler/api/devices/config"
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/gpushare"
	"volcano.sh/volcano/pkg/scheduler/api/devices/nvidia/vgpu"
	"volcano.sh/volcano/pkg/scheduler/framework"
	vnpu310p "volcano.sh/volcano/pkg/scheduler/plugins/deviceshare/devices/ascend/310p/vnpu"
)

const (
	// PluginName 插件名称，用于在调度器中注册和识别该插件
	PluginName = "deviceshare"

	// GPUSharingPredicate 在 YAML 配置中启用 GPU 共享预选策略的配置键
	GPUSharingPredicate = "deviceshare.GPUSharingEnable"

	// NodeLockEnable 启用节点锁定功能的配置键，防止并发调度冲突
	NodeLockEnable = "deviceshare.NodeLockEnable"

	// GPUNumberPredicate 启用 GPU 数量预选策略的配置键
	GPUNumberPredicate = "deviceshare.GPUNumberEnable"

	// VGPUEnable 启用虚拟 GPU (vGPU) 功能的配置键
	VGPUEnable = "deviceshare.VGPUEnable"

	// ASCEND310PvGPU 启用华为昇腾 310P 虚拟 NPU 功能的配置键
	ASCEND310PvGPU = "deviceshare.ASCEND310PVNPUEnable"

	// SchedulePolicyArgument 调度策略参数的配置键（如 binpack 或 spread）
	SchedulePolicyArgument = "deviceshare.SchedulePolicy"

	// ScheduleWeight 调度权重的配置键，用于计算节点得分时的权重系数
	ScheduleWeight = "deviceshare.ScheduleWeight"

	// KnownGeometriesCMName vGPU 设备配置的 ConfigMap 名称的配置键
	KnownGeometriesCMName = "deviceshare.KnownGeometriesCMName"

	// KnownGeometriesCMNamespace vGPU 设备配置的 ConfigMap 所在命名空间的配置键
	KnownGeometriesCMNamespace = "deviceshare.KnownGeometriesCMNamespace"
)

// 设备共享插件结构体，实现调度器插件接口
type deviceSharePlugin struct {
	// pluginArguments 插件的配置参数，从调度器配置中传入
	pluginArguments framework.Arguments

	// schedulePolicy 设备调度策略
	schedulePolicy string

	// scheduleWeight 设备调度权重
	scheduleWeight int
}

// 创建并返回 deviceSharePlugin 实例，由调度框架在启动时调用
func New(arguments framework.Arguments) framework.Plugin {
	// 创建插件实例，初始化调度策略为空字符串，权重为 0
	dsp := &deviceSharePlugin{pluginArguments: arguments, schedulePolicy: "", scheduleWeight: 0}

	// 调用 enablePredicate 函数处理插件配置参数
	enablePredicate(dsp)

	return dsp
}

func (dp *deviceSharePlugin) Name() string {
	return PluginName
}

// 从配置中读取各种设备相关的启用标志和参数，并执行必要的冲突检查和初始化工作
func enablePredicate(dsp *deviceSharePlugin) {
	// 初始化节点锁定标志
	nodeLockEnable := false

	// 获取插件参数对象
	args := dsp.pluginArguments

	// 从配置中读取 GPU 共享启用标志
	args.GetBool(&gpushare.GpuSharingEnable, GPUSharingPredicate)

	// 从配置中读取 GPU 数量模式启用标志
	args.GetBool(&gpushare.GpuNumberEnable, GPUNumberPredicate)

	// 从配置中读取节点锁定启用标志
	args.GetBool(&nodeLockEnable, NodeLockEnable)

	// 从配置中读取 vGPU 启用标志
	args.GetBool(&vgpu.VGPUEnable, VGPUEnable)

	// 从配置中读取昇腾 310P vNPU 启用标志
	args.GetBool(&vnpu.Ascend310pvNPUEnable, ASCEND310PvGPU)

	// 将节点锁定配置应用到 gpushare 包
	gpushare.NodeLockEnable = nodeLockEnable

	// 将节点锁定配置应用到 vgpu 包
	vgpu.NodeLockEnable = nodeLockEnable

	// 从配置中读取调度策略（如 binpack 或 spread）
	args.GetString(&dsp.schedulePolicy, SchedulePolicyArgument)

	// 从配置中读取调度权重
	args.GetInt(&dsp.scheduleWeight, ScheduleWeight)

	// 互斥性检查：GPU 共享模式和 GPU 数量模式不能同时启用
	if gpushare.GpuSharingEnable && gpushare.GpuNumberEnable {
		klog.Fatal("can not define true in both gpu sharing and gpu number")
	}

	// 互斥性检查：GPU 模式（共享或数量）与 vGPU 模式不能同时启用
	if (gpushare.GpuSharingEnable || gpushare.GpuNumberEnable) && vgpu.VGPUEnable {
		klog.Fatal("gpu-share and vgpu can't be used together")
	}

	// 如果未启用 vGPU，则直接返回，不需要后续的 vGPU 配置初始化
	if !vgpu.VGPUEnable {
		return
	}

	// vGPU 配置初始化：设置已知几何配置的 ConfigMap 名称
	knownGeometriesCMName := "volcano-vgpu-device-config"
	args.GetString(&knownGeometriesCMName, KnownGeometriesCMName)

	// vGPU 配置初始化：设置已知几何配置的 ConfigMap 命名空间
	knownGeometriesCMNamespace := "kube-system"
	args.GetString(&knownGeometriesCMNamespace, KnownGeometriesCMNamespace)

	// 使用 ConfigMap 信息初始化设备配置
	config.InitDevicesConfig(knownGeometriesCMName, knownGeometriesCMNamespace)
}

// 创建并返回一个状态对象表示调度操作的结果状态
func createStatus(code int, reason string) *api.Status {
	status := api.Status{
		Code:   code,  
		Reason: reason, 
	}
	return &status
}

// 计算单个节点的 vGPU 和 GPU 设备得分
func getDeviceScore(ctx context.Context, pod *v1.Pod, node *api.NodeInfo, schedulePolicy string) (int64, *fwk.Status) {
	s := float64(0)

	// 遍历节点上的所有设备类型
	for deviceType, device := range node.Others {
		// 如果 pod 请求该设备类型
		if device.(api.Devices).HasDeviceRequest(pod) {
			var ns float64

			// 调用 vGPU 和 GPU 的 ScoreNode 方法计算对应设备类型的得分
			if deviceType == vgpu.DeviceName || deviceType == gpushare.DeviceName {
				// ScoreNode 是接口方法，具体实现由不同设备类型决定
				ns = device.(api.Devices).ScoreNode(pod, schedulePolicy)
			} else {
				// vNPU 设备使用 BatchNodeOrderFn 计算，在这里跳过
				continue
			}

			s += ns
		}
	}

	klog.V(4).Infof("deviceScore for task %s/%s is: %v", pod.Namespace, pod.Name, s)

	return int64(math.Floor(s + 0.5)), nil
}

// 批量计算多个节点的 vNPU 设备得分
func getDeviceScoresInBatch(pod *v1.Pod, schedulePolicy string, allDevices []api.Devices) []float64 {
	switch d := allDevices[0].(type) {
	case *vnpu.NPUDevices:
		// vNPU 设备使用单独的批量打分函数
		return vnpu310p.ScoreBatchNodes(pod, schedulePolicy, d, allDevices)
	default:
		score := make([]float64, 0)
		return score
	}
}

// 初始化节点得分映射表，为每个节点创建初始得分为 0.0 的映射
func initScoreMap(nodes []*api.NodeInfo) map[string]float64 {
	// 创建映射表，key 节点名称，value 得分，容量为节点数量
	scoreMap := make(map[string]float64, len(nodes))

	// 遍历所有节点
	for _, node := range nodes {
		// 跳过空节点
		if reflect.ValueOf(node).IsNil() {
			continue
		}		
		scoreMap[node.Name] = 0.0
	}
	return scoreMap
}

// 在调度会话开启时，为每个节点上的每种设备类型执行初始化
// ssn: 调度会话对象，包含全局调度信息
func initializeDevicesWithSession(ssn *framework.Session) {
	// 遍历会话中的所有节点
	for _, nodeInfo := range ssn.Nodes {
		// 遍历所有已注册的设备类型
		for _, val := range api.RegisteredDevices {
			// 尝试获取节点上的设备对象
			if dev, ok := nodeInfo.Others[val].(api.Devices); ok {
				// 调用设备初始化函数
				if err := initializeDevice(dev, ssn, nodeInfo); err != nil {
					klog.Warningf("Failed to initialize devices with session for node %s: %v", nodeInfo.Name, err)
				}
			}
		}
	}
}

// 使用类型开关为不同的设备实现调用对应的初始化函数
// device: 设备接口对象
// ssn: 调度会话对象
// nodeInfo: 节点信息对象
func initializeDevice(device api.Devices, ssn *framework.Session, nodeInfo *api.NodeInfo) error {
	switch d := device.(type) {
	case *vnpu.NPUDevices:
		// vNPU 设备的初始化
		klog.V(3).Infof("initialize ascend310p device.")
		return vnpu310p.InitVNPUDevice(d, ssn, nodeInfo)
	default:
		// 其他设备类型暂不需要特殊初始化
		return nil
	}
}

// OnSessionOpen 在调度会话开启时被调用，注册谓词函数和节点打分函数
// 实现 framework.Plugin 接口的 OnSessionOpen 方法
// ssn: 当前的调度会话对象
func (dp *deviceSharePlugin) OnSessionOpen(ssn *framework.Session) {
	// 第一步：初始化需要调度会话信息的设备，对于 vNPU，可能需要调度会话的全局信息
	initializeDevicesWithSession(ssn)

	// 第二步：注册谓词，根据节点上的设备是否满足 Pod 资源请求，判断 Pod 是否可以调度到当前节点
	ssn.AddPredicateFn(dp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) error {
		// 创建谓词状态列表，用于收集过滤结果
		predicateStatus := make([]*api.Status, 0)

		// 遍历所有已注册的设备类型
		for _, val := range api.RegisteredDevices {
			// 检查节点是否有该类型的设备，从节点的 Others 字段中获取该类型的设备对象
			if dev, ok := node.Others[val].(api.Devices); ok {
				// 检查设备对象是否为 nil
				if reflect.ValueOf(dev).IsNil() {
					// 如果设备为 nil，表示节点上未初始化该类型的设备，但 Pod 请求该设备，则返回不可调度
					if dev == nil || dev.HasDeviceRequest(task.Pod) {
						predicateStatus = append(predicateStatus, &api.Status{
							Code:   devices.Unschedulable,
							Reason: "node not initialized with device" + val,
							Plugin: PluginName,
						})
						return api.NewFitErrWithStatus(task, node, predicateStatus...)
					}
					klog.V(4).Infof("pod %s/%s did not request device %s on %s, skipping it",
					    task.Pod.Namespace, task.Pod.Name, val, node.Name)
					continue
				}

				// dev.FilterNode 是接口方法，具体实现由不同设备类型决定
				// FilterNode 根据要调度的 Pod 对象和调度策略进行过滤，判断 Pod 是否可以调度到当前节点
				code, msg, err := dev.FilterNode(task.Pod, dp.schedulePolicy)

				// 如果过滤过程中发生错误，返回不可调度
				if err != nil {
					predicateStatus = append(predicateStatus, createStatus(code, msg))
					return api.NewFitErrWithStatus(task, node, predicateStatus...)
				}

				// 创建过滤状态对象
				filterNodeStatus := createStatus(code, msg)

				// 如果过滤结果不是成功，返回不可调度
				if filterNodeStatus.Code != api.Success {
					predicateStatus = append(predicateStatus, filterNodeStatus)
					return api.NewFitErrWithStatus(task, node, predicateStatus...)
				}
			} else {
				// 设备类型断言失败，记录警告并跳过
				klog.Warningf("Devices %s assertion conversion failed, skip", val)
			}
		}

		// 所有设备检查通过，记录 Pod 可以调度到该节点
		klog.V(4).Infof("checkDevices predicates Task <%s/%s> on Node <%s>: fit ",
			task.Namespace, task.Name, node.Name)

		return nil
	})

	// 第三步：注册节点打分
	ssn.AddNodeOrderFn(dp.Name(), func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
		nodeScore := float64(0)

		// 只有在调度权重大于 0 时才计算设备得分
		if dp.scheduleWeight > 0 {
			// 基于 pod 请求和设备类型计算设备得分
			score, status := getDeviceScore(context.TODO(), task.Pod, node, dp.schedulePolicy)

			if !status.IsSuccess() {
				klog.Warningf("Node: %s, Calculate Device Score Failed because of Error: %v",
				    node.Name, status.AsError())
				return 0, status.AsError()
			}

			// 节点得分 = 设备得分 × 调度权重
			// TODO: 应该为设备创建单独的插件，将它们从谓词和节点排序插件中分离出来
			nodeScore = float64(score) * float64(dp.scheduleWeight)

			klog.V(5).Infof("Node: %s, task<%s/%s> Device Score weight %d, score: %f",
			    node.Name, task.Namespace, task.Name, dp.scheduleWeight, nodeScore)
		}

		return nodeScore, nil
	})

	// 第四步：注册批量节点打分函数，用于 vNPU 的批量打分
	ssn.AddBatchNodeOrderFn(dp.Name(), func(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
		scoreMap := initScoreMap(nodes)

		if dp.scheduleWeight > 0 {
			// 遍历所有已注册的设备类型
			for _, deviceType := range api.RegisteredDevices {
				// 如果设备类型不是 vNPU，跳过
				if deviceType != vnpu.DeviceName {
					continue
				}

				// 收集所有节点上的 vNPU 类型设备
				allDevices := make([]api.Devices, 0)

				// 遍历所有节点
				for _, node := range nodes {
					// 获取节点上的设备对象
					device, ok := node.Others[deviceType]
					if ok {
						// 尝试将对象转换为设备接口
						if deviceInterface, isDeviceInterface := device.(api.Devices); isDeviceInterface {
							if reflect.ValueOf(deviceInterface).IsNil() {
								// 如果设备为 nil，但 Pod 请求该设备，返回错误
								if deviceInterface == nil || deviceInterface.HasDeviceRequest(task.Pod) {
									return nil, fmt.Errorf("node not initialized with device %s", deviceType)
								}
								// Pod 没有请求该设备，跳过
								klog.V(4).Infof("pod %s/%s did not request device %s on %s, skipping it",
								    task.Pod.Namespace, task.Pod.Name, deviceType, nodes[0].Name)
								continue
							}
							// 将设备添加到列表中
							allDevices = append(allDevices, deviceInterface)
						}
					} else {
						klog.Warningf("Devices %s assertion conversion failed, skip", deviceType)
					}
				}

				// 检查是否有可用于打分的设备
				if len(allDevices) == 0 {
					klog.V(4).Infof("No devices of type %s found for scoring", deviceType)
					continue
				}

				// 批量获取所有节点的设备得分
				scores := getDeviceScoresInBatch(task.Pod, dp.schedulePolicy, allDevices)

				// 确保得分数组长度与节点数量匹配
				if len(scores) != len(nodes) {
					klog.Warningf("Score array length (%d) doesn't match nodes length (%d) for device type %s",
					    len(scores), len(nodes), deviceType)
					continue
				}

				// 计算节点得分并更新得分映射表
				for i := range nodes {
					// 节点得分 = 设备得分 × 调度权重
					finalScore := scores[i] * float64(dp.scheduleWeight)
					// 更新节点得分
					scoreMap[nodes[i].Node.Name] += finalScore
					klog.V(5).Infof("Node: %s, task<%s/%s> Device Score weight %d, score: %f",
					    nodes[i].Name, task.Namespace, task.Name, dp.scheduleWeight, finalScore)
				}
			}
		}

		return scoreMap, nil
	})
}

// OnSessionClose 在调度会话关闭时被调用
// 实现 framework.Plugin 接口的 OnSessionClose 方法
// 当前实现为空，因为设备共享插件不需要在会话关闭时执行清理操作
// ssn: 当前的调度会话对象
func (dp *deviceSharePlugin) OnSessionClose(ssn *framework.Session) {}
