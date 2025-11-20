/*
Copyright 2023 The Volcano Authors.

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

package vgpu

import (
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api/devices"
	deviceconfig "volcano.sh/volcano/pkg/scheduler/api/devices/config"
	"volcano.sh/volcano/pkg/scheduler/plugins/util/nodelock"
)

type GPUUsage struct {
	UsedMem  uint
	UsedCore uint
}

// GPUDevice include gpu id, memory and the pods that are sharing it.
type GPUDevice struct {
	// GPU ID
	ID int
	// Node this GPU Device belongs
	Node string
	// GPU Unique ID
	UUID string
	// The resource usage by pods that are sharing this GPU
	PodMap map[string]*GPUUsage
	// memory per card
	Memory uint
	// max sharing number
	Number uint
	// type of this number
	Type string
	// Health condition of this GPU
	Health bool
	// number of allocated
	UsedNum uint
	// number of device memory allocated
	UsedMem uint
	// number of core used
	UsedCore uint
	// MigTemplate for this GPU
	MigTemplate []deviceconfig.Geometry
	/// MigUsage for this GPU
	MigUsage deviceconfig.MigInUse
}

type GPUDevices struct {
	Name string
	// Mode GPU sharing mode
	Mode string
	// We cache score in filter step according to schedulePolicy, to avoid recalculating in score
	Score float64

	Device map[int]*GPUDevice
	// Sharing sharing handler
	Sharing SharingFactory
}

// NewGPUDevice 创建一个新的GPU设备实例
// 参数 id: 设备的唯一标识符
// 参数 mem: 设备的内存容量（以MB为单位）
// 返回初始化完成的GPUDevice结构体指针
func NewGPUDevice(id int, mem uint) *GPUDevice {
	// 创建并返回一个新的GPUDevice结构体
	return &GPUDevice{
		ID:       id,                        // 设置设备ID
		Memory:   mem,                       // 设置设备内存容量
		PodMap:   make(map[string]*GPUUsage), // 初始化Pod映射，用于记录使用该设备的Pod
		UsedNum:  0,                         // 初始已使用实例数量为0
		UsedMem:  0,                         // 初始已使用内存为0
		UsedCore: 0,                         // 初始已使用核心为0
	}
}

// NewGPUDevices 从节点注解信息创建GPU设备集合
// 参数 name: 节点名称
// 参数 node: Kubernetes节点对象
// 返回GPUDevices集合，包含该节点上所有的GPU设备信息
func NewGPUDevices(name string, node *v1.Node) *GPUDevices {
	// 检查节点对象是否为空，为空则无法创建设备
	if node == nil {
		return nil
	}
	// 从节点注解中获取GPU设备注册信息
	// VolcanoVGPURegister注解包含了该节点上所有GPU设备的详细信息
	annos, ok := node.Annotations[deviceconfig.VolcanoVGPURegister]
	if !ok {
		// 如果没有注册信息，说明该节点没有配置vGPU设备
		return nil
	}
	// 从节点注解中获取握手信息
	// VolcanoVGPUHandshake用于调度器和节点之间的通信同步
	handshake, ok := node.Annotations[deviceconfig.VolcanoVGPUHandshake]
	if !ok {
		// 如果没有握手信息，说明设备还未准备好
		return nil
	}
	// 解析节点设备注册信息，创建GPUDevices结构体
	// decodeNodeDevices会将字符串格式的设备信息解析为结构化数据
	nodedevices, sharingMode := decodeNodeDevices(name, annos)
	// 检查解析结果是否有效
	if (nodedevices == nil) || len(nodedevices.Device) == 0 {
		// 如果设备列表为空，说明没有可用的GPU设备
		return nil
	}
	// 根据共享模式获取对应的处理器
	// GetSharingHandler返回对应共享模式的实例（HAMI-core或MIG）
	sharingHandler, _ := GetSharingHandler(sharingMode)
	// 记录GPU共享模式到日志（日志级别3）
	klog.V(3).Infoln("GPU sharing mode: ", sharingMode)
	// 遍历所有设备，初始化设备指标
	for _, val := range nodedevices.Device {
		// 记录每个注册的NVIDIA设备信息到日志
		klog.V(3).InfoS("Nvidia Device registered name", "name", nodedevices.Name, "val", *val)
		// 重置设备指标，用于监控和统计
		ResetDeviceMetrics(val.UUID, node.Name, float64(val.Memory))
	}

	// 执行握手协议以避免调度器和节点之间的时间不一致
	if strings.Contains(handshake, "Requesting") {
		// 如果握手状态为"请求中"，解析请求时间
		// 握手信息格式：Requesting_2006.01.02 15:04:05
		formertime, _ := time.Parse("2006.01.02 15:04:05", strings.Split(handshake, "_")[1])
		// 检查请求是否已超时（60秒）
		if time.Now().After(formertime.Add(time.Second * 60)) {
			// 记录节点设备超时离开的信息
			klog.V(3).Infof("node %v device %s leave", node.Name, handshake)

			// 创建临时注解，标记设备为已删除状态
			tmppat := make(map[string]string)
			tmppat[deviceconfig.VolcanoVGPUHandshake] = "Deleted_" + time.Now().Format("2006.01.02 15:04:05")
			// 更新节点注解，通知其他组件设备已删除
			patchNodeAnnotations(node, tmppat)
			return nil
		}
	} else if strings.Contains(handshake, "Deleted") {
		// 如果设备已被标记为删除，直接返回nil
		return nil
	} else {
		// 如果没有握手状态，设置请求状态
		tmppat := make(map[string]string)
		tmppat[deviceconfig.VolcanoVGPUHandshake] = "Requesting_" + time.Now().Format("2006.01.02 15:04:05")
		// 更新节点注解，发起握手请求
		patchNodeAnnotations(node, tmppat)
	}
	// 将共享处理器赋值给设备集合
	nodedevices.Sharing = sharingHandler
	// 返回完整的GPU设备集合
	return nodedevices
}

// ScoreNode 根据调度策略计算节点得分
// 参数 pod: 要调度的Pod对象
// 参数 schedulePolicy: 调度策略（如binpack或spread）
// 返回节点的得分值，用于排序选择最优节点
func (gs *GPUDevices) ScoreNode(pod *v1.Pod, schedulePolicy string) float64 {
	/* TODO: 我们需要一个基础得分来与抢占机制兼容，
	   这意味着不需要驱逐任务的节点应该比需要驱逐任务的节点得分更高 */

	// 使用在过滤阶段缓存的得分，避免重复计算
	// 在FilterNode阶段已经计算过得分并存储在gs.Score中
	return gs.Score
}

// GetIgnoredDevices 返回需要忽略的设备列表
// 在vGPU场景中，暂时没有需要忽略的设备，因此返回空切片
// 返回空字符串切片
func (gs *GPUDevices) GetIgnoredDevices() []string {
	// 当前实现中没有需要忽略的设备，返回空切片
	return []string{}
}

// AddQueueResource 添加Pod的资源到队列资源统计中
// 这个函数用于统计队列中已分配的资源
// 参数 pod: 需要统计资源的Pod对象
// 返回资源映射，包含该Pod使用的GPU内存和核心资源量
func (gs *GPUDevices) AddQueueResource(pod *v1.Pod) map[string]float64 {
	// 检查GPU设备集合是否为空
	if gs == nil {
		// 如果为空，返回空资源映射
		return map[string]float64{}
	}
	// 记录进入函数的日志（日志级别5）
	klog.V(5).InfoS("AddQueueResource", "Name", pod.Name)
	// 创建资源映射，用于存储Pod使用的资源量
	res := map[string]float64{}
	// 从Pod注解中获取已分配的设备ID列表
	// AssignedIDsAnnotations注解包含了Pod实际分配到的GPU设备信息
	ids, ok := pod.Annotations[AssignedIDsAnnotations]
	if !ok {
		// 如果没有设备分配信息，记录错误并返回空映射
		klog.Errorf("pod %s has no annotation volcano.sh/devices-to-allocate", pod.Name)
		return res
	}
	// 解析Pod设备分配信息字符串，转换为结构化数据
	podDev := decodePodDevices(ids)
	// 遍历每个容器的设备分配
	for _, val := range podDev {
		// 遍历容器中的每个设备
		for _, deviceused := range val {
			// 遍历节点上的所有GPU设备，查找匹配的设备
			for _, gsdevice := range gs.Device {
				// 通过UUID匹配设备
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
					// 将GPU内存使用量添加到资源映射中，单位转换为千（MB×1000）
					res[getConfig().ResourceMemoryName] += float64(deviceused.Usedmem * 1000)
					// 将GPU核心使用量添加到资源映射中，单位转换为千（百分比×1000）
					res[getConfig().ResourceCoreName] += float64(deviceused.Usedcores * 1000)
				}
			}
		}
	}
	// 记录统计结果到日志（日志级别4）
	klog.V(4).InfoS("AddQueueResource", "Name=", pod.Name, "res=", res)
	// 返回Pod使用的资源映射
	return res
}

// AddResource 将已分配的Pod添加到GPU资源池中
// 当Pod被成功分配到某个节点的GPU设备后，需要更新节点的资源使用状态
// 参数 pod: 已经GPU分配的Pod对象
func (gs *GPUDevices) AddResource(pod *v1.Pod) {
	// 检查GPU设备集合是否为空
	if gs == nil {
		// 如果为空，无法添加资源，直接返回
		return
	}

	// 调用内部的addResource方法，传入Pod的注解和Pod对象
	// 注解中包含了设备分配信息
	gs.addResource(pod.Annotations, pod)
}

// addResource 内部方法，实际执行资源添加逻辑
// 参数 annotations: Pod的注解映射，包含设备分配信息
// 参数 pod: 要添加到资源的Pod对象
func (gs *GPUDevices) addResource(annotations map[string]string, pod *v1.Pod) {
	// 从注解中获取Pod分配到的设备ID列表
	// AssignedIDsAnnotations包含了分配给该Pod的GPU设备信息
	ids, ok := annotations[AssignedIDsAnnotations]
	if !ok {
		// 如果没有设备分配信息，记录错误
		klog.Errorf("pod %s has no annotation volcano.sh/devices-to-allocate", pod.Name)
		return
	}
	// 解析Pod设备分配信息字符串
	podDev := decodePodDevices(ids)
	// 遍历每个容器的设备分配
	for _, val := range podDev {
		// 遍历容器中的每个设备
		for _, deviceused := range val {
			// 遍历节点上的所有GPU设备，找到对应的设备
			for index, gsdevice := range gs.Device {
				// 通过UUID匹配设备
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
					// 调用共享处理器的AddPod方法，将Pod添加到设备
					// 参数：设备、使用的内存、使用的核心、Pod UID、设备UUID
					err := gs.Sharing.AddPod(gsdevice, deviceused.Usedmem, deviceused.Usedcores, string(pod.UID), deviceused.UUID)
					if err == nil {
						// 如果添加成功，更新设备指标
						// 记录Pod与设备的关联关系，用于监控
						gs.AddPodMetrics(index, string(pod.UID), pod.Name)
					} else {
						// 如果添加失败，记录错误日志
						klog.ErrorS(err, "add resource failed")
					}
					// 找到匹配的设备后，跳出内层循环
					break
				}
			}
		}
	}
}

// SubResource 释放Pod占用的GPU资源
// 当Pod结束运行或被删除时，需要从设备中移除其占用的资源
// 参数 pod: 要释放资源的Pod对象
func (gs *GPUDevices) SubResource(pod *v1.Pod) {
	// 检查GPU设备集合是否为空
	if gs == nil {
		// 如果为空，无法释放资源，直接返回
		return
	}
	// 从Pod注解中获取已分配的设备ID列表
	ids, ok := pod.Annotations[AssignedIDsAnnotations]
	if !ok {
		// 如果没有设备分配信息，直接返回
		return
	}
	// 解析Pod设备分配信息字符串
	podDev := decodePodDevices(ids)
	// 遍历每个容器的设备分配
	for _, val := range podDev {
		// 遍历容器中的每个设备
		for _, deviceused := range val {
			// 遍历节点上的所有GPU设备，找到对应的设备
			for index, gsdevice := range gs.Device {
				// 通过UUID匹配设备
				if strings.Contains(deviceused.UUID, gsdevice.UUID) {
					// 调用共享处理器的SubPod方法，从设备中移除Pod
					// 参数：设备、使用的内存、使用的核心、Pod UID、设备UUID
					err := gs.Sharing.SubPod(gsdevice, uint(deviceused.Usedmem), uint(deviceused.Usedcores), string(pod.UID), deviceused.UUID)
					if err != nil {
						// 如果释放失败，记录错误日志
						klog.ErrorS(err, "sub resource failed")
					} else {
						// 如果释放成功，更新设备指标
						// 移除Pod与设备的关联关系，用于监控
						gs.SubPodMetrics(index, string(pod.UID), pod.Name)
					}
					// 找到匹配的设备后，跳出内层循环
					break
				}
			}
		}
	}
}

// HasDeviceRequest 检查Pod是否有GPU设备请求
// 参数 pod: 要检查的Pod对象
// 返回布尔值，如果有GPU设备请求则返回true，否则返回false
func (gs *GPUDevices) HasDeviceRequest(pod *v1.Pod) bool {
	// 检查是否启用了VGPU功能并且Pod有VGPU资源请求
	if VGPUEnable && checkVGPUResourcesInPod(pod) {
		// 如果启用且有请求，返回true
		return true
	}
	// 否则返回false
	return false
}

// Release 释放Pod占用的GPU设备资源
// 参数 kubeClient: Kubernetes客户端接口
// 参数 pod: 要释放资源的Pod对象
// 返回操作过程中可能出现的错误
func (gs *GPUDevices) Release(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	// 当前实现中，Release方法是一个占位符，直接返回nil
	// 实际的资源释放逻辑在SubResource方法中实现
	return nil
}

// 根据节点上的 vGPU 设备是否满足 Pod 资源请求，判断 Pod 是否可以调度到当前节点
func (gs *GPUDevices) FilterNode(pod *v1.Pod, schedulePolicy string) (int, string, error) {
	// 检查是否启用 VGPU 功能
	if VGPUEnable {
		klog.V(4).Infoln("hami-vgpu DeviceSharing starts filtering pods", pod.Name)
		// 调用checkNodeGPUSharingPredicateAndScore进行过滤和打分
		// 参数：Pod、设备集合、是否创建副本、调度策略
		// true表示创建设备快照进行模拟，不会修改实际的设备状态
		fit, _, score, err := checkNodeGPUSharingPredicateAndScore(pod, gs, true, schedulePolicy)
		if err != nil || !fit {
			// 如果过滤失败或不满足条件，记录错误日志
			klog.ErrorS(err, "Failed to fitler node to vgpu task", "pod", pod.Name)
			return devices.Unschedulable, "hami-vgpuDeviceSharing error", err
		}
		// 将计算出的得分保存到 GPUDevices 结构体的 Score 字段中，在后续的 NodeOrder 阶段使用
		gs.Score = score
		klog.V(4).Infoln("hami-vgpu DeviceSharing successfully filters pods")
	}
	return devices.Success, "", nil
}

// Allocate 分配GPU设备给Pod
// 参数 kubeClient: Kubernetes客户端接口
// 参数 pod: 要分配设备的Pod对象
// 返回分配过程中可能出现的错误
func (gs *GPUDevices) Allocate(kubeClient kubernetes.Interface, pod *v1.Pod) error {
	// 检查是否启用了VGPU功能
	if VGPUEnable {
		// 记录开始分配的日志（日志级别4）
		klog.V(4).Infoln("hami-vgpu DeviceSharing:Into AllocateToPod", pod.Name)
		// 再次调用checkNodeGPUSharingPredicateAndScore，这次不创建副本（replicate=false）
		// 返回实际的设备分配结果（第二个参数device）
		fit, device, _, err := checkNodeGPUSharingPredicateAndScore(pod, gs, false, "")
		if err != nil || !fit {
			// 如果分配失败，记录错误并返回
			klog.ErrorS(err, "Failed to allocate vgpu task", "pod", pod.Name)
			return err
		}
		// 如果启用了节点锁功能，需要先获取节点锁
		if NodeLockEnable {
			// 设置节点锁使用的Kubernetes客户端
			nodelock.UseClient(kubeClient)
			// 尝试获取节点锁，防止多个调度器同时修改节点状态
			err = nodelock.LockNode(gs.Name, DeviceName)
			if err != nil {
				// 如果获取锁失败，返回详细错误信息
				return errors.Errorf("node %s locked for %s hamivgpu lockname %s", gs.Name, pod.Name, err.Error())
			}
		}

		// 创建注解映射，用于存储分配信息
		annotations := make(map[string]string)
		// 记录分配到的节点名称
		annotations[AssignedNodeAnnotations] = gs.Name
		// 记录分配时间（Unix时间戳）
		annotations[AssignedTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)
		// 记录分配到的设备列表（编码后的字符串）
		annotations[AssignedIDsAnnotations] = encodePodDevices(device)
		// 保存设备分配信息的副本
		annotations[AssignedIDsToAllocateAnnotations] = annotations[AssignedIDsAnnotations]

		// 设置设备绑定阶段为"分配中"
		annotations[DeviceBindPhase] = "allocating"
		// 记录绑定时间（Unix时间戳）
		annotations[BindTimeAnnotations] = strconv.FormatInt(time.Now().Unix(), 10)
		// 为了避免Pod分配信息更新延迟，先更新本地缓存
		gs.addResource(annotations, pod)
		// 更新Pod的注解，将分配信息写入Kubernetes API
		err = patchPodAnnotations(kubeClient, pod, annotations)
		if err != nil {
			// 如果更新Pod注解失败，返回错误
			return err
		}

		// 记录分配成功的日志（日志级别3）
		klog.V(3).Infoln("DeviceSharing:Allocate Success")
	}
	// 如果没有启用VGPU或分配成功，返回nil（无错误）
	return nil
}
