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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api/devices"
	"volcano.sh/volcano/pkg/scheduler/api/devices/config"
)

// extractGeometryFromType 从GPU设备类型字符串中提取MIG几何配置
// 参数 t 是GPU设备的类型字符串，例如 "NVIDIA-A100-SXM4-40GB"
// 返回该设备类型对应的MIG几何配置切片和可能的错误
func extractGeometryFromType(t string) ([]config.Geometry, error) {
	// 获取全局配置对象，检查配置是否存在
	if config.GetConfig() != nil {
		// 遍历配置中所有的MIG几何配置列表
		// MigGeometriesList包含了不同GPU型号的MIG切片配置
		for _, val := range config.GetConfig().NvidiaConfig.MigGeometriesList {
			found := false
			// 遍历当前配置项支持的GPU型号列表
			// 检查传入的设备类型是否匹配配置中的任一型号
			for _, migDevType := range val.Models {
				// 使用字符串包含匹配，支持模糊匹配GPU型号
				if strings.Contains(t, migDevType) {
					found = true
				}
			}
			// 如果找到匹配的GPU型号，返回对应的MIG几何配置
			// Geometries描述了如何将物理GPU分割成多个MIG实例
			if found {
				return val.Geometries, nil
			}
		}
	}
	// 如果遍历完所有配置都未找到匹配，返回空切片和错误
	return []config.Geometry{}, errors.New("mig type not found")
}

// decodeNodeDevices 解析节点上的GPU设备信息，创建GPUDevices结构体
// 参数 name: 节点名称
// 参数 str: 格式化的设备信息字符串，例如 "UUID,count,memory,type,health,mode:..."
// 返回解析后的GPUDevices结构体指针和节点的GPU共享模式
func decodeNodeDevices(name, str string) (*GPUDevices, string) {
	// 检查输入字符串是否包含冒号分隔符，冒号用于分隔不同的GPU设备信息
	if !strings.Contains(str, ":") {
		return nil, ""
	}
	// 按冒号分割字符串，获取各个GPU设备的信息
	tmp := strings.Split(str, ":")
	// 创建新的GPUDevices结构体
	retval := &GPUDevices{
		Name:   name,               // 设置节点名称
		Device: make(map[int]*GPUDevice), // 初始化设备映射
		Score:  float64(0),         // 初始化节点得分为0
	}
	// 默认使用HAMI-core作为GPU共享模式
	sharingMode := vGPUControllerHAMICore
	// 遍历解析后的每个设备信息
	for index, val := range tmp {
		// 检查当前设备信息是否包含逗号分隔符，逗号用于分隔设备的属性
		if strings.Contains(val, ",") {
			// 按逗号分割设备属性
			items := strings.Split(val, ",")
			// 验证设备信息格式是否完整，至少需要6个属性
			if len(items) < 6 {
				klog.Error("wrong Node GPU info: ", val)
				return nil, ""
			}
			// 解析设备的共享实例数量
			count, _ := strconv.Atoi(items[1])
			// 解析设备的内存容量(MB)
			devmem, _ := strconv.Atoi(items[2])
			// 解析设备的健康状态
			health, _ := strconv.ParseBool(items[4])
			// 创建新的GPUDevice结构体，填充解析的信息
			i := GPUDevice{
				ID:          index,        // 设备索引
				Node:        name,         // 所属节点名称
				UUID:        items[0],     // 设备的唯一标识符
				Number:      uint(count),  // 设备的共享实例数量
				Memory:      uint(devmem), // 设备的内存容量
				Type:        items[3],     // 设备类型
				PodMap:      make(map[string]*GPUUsage), // 初始化Pod使用记录映射
				Health:      health,       // 设备健康状态
				MigTemplate: []config.Geometry{}, // 初始化MIG模板为空
				MigUsage: config.MigInUse{   // 初始化MIG使用情况
					Index: -1},           // 设置MIG索引为-1，表示未使用
			}
			// 解析共享模式（HAMI-core或MIG）
			sharingMode = getSharingMode(items[5])
			// 如果是MIG模式，则需要提取对应的MIG几何配置
			if sharingMode == vGPUControllerMIG {
				var err error
				// 调用extractGeometryFromType获取设备对应的MIG几何配置
				i.MigTemplate, err = extractGeometryFromType(i.Type)
				// 如果提取失败，回退到HAMI-core模式
				if err != nil {
					sharingMode = vGPUControllerHAMICore
					klog.ErrorS(err, "extract mig geometry error and fall back to hamicore mode")
				}
			}
			// 将设备添加到映射中
			retval.Device[index] = &i
		}
	}
	// 设置节点的共享模式
	retval.Mode = sharingMode
	// 返回解析的设备结构体和共享模式
	return retval, sharingMode
}

// encodeContainerDevices 将容器的设备分配信息编码成字符串
// 参数 cd: 容器分配的设备列表
// 返回编码后的字符串，格式为 "UUID,Type,UsedMem,UsedCores:"，每个设备信息之间用冒号分隔
func encodeContainerDevices(cd []ContainerDevice) string {
	tmp := ""
	// 遍历所有分配给容器的设备
	for _, val := range cd {
		// 将每个设备的信息拼接成字符串
		// 格式：UUID,设备类型,已使用内存量,已使用核心百分比:
		tmp += val.UUID + "," + val.Type + "," +
		       strconv.Itoa(int(val.Usedmem)) + "," +
		       strconv.Itoa(int(val.Usedcores)) + ":"
	}
	// 记录编码后的设备信息字符串到日志（日志级别4）
	klog.V(4).Infoln("Encoded container Devices=", tmp)
	// 返回编码后的字符串
	return tmp
	//return strings.Join(cd, ",")  // 注释掉的旧版实现
}

// encodePodDevices 将Pod中所有容器的设备分配信息编码成字符串
// 参数 pd: Pod中所有容器的设备分配列表
// 返回编码后的字符串，多个容器的设备信息用分号分隔
func encodePodDevices(pd []ContainerDevices) string {
	// 创建字符串切片，用于存储每个容器的编码结果
	var ss []string
	// 遍历Pod中每个容器的设备分配
	for _, cd := range pd {
		// 调用encodeContainerDevices对每个容器的设备信息进行编码
		// 将编码结果添加到字符串切片中
		ss = append(ss, encodeContainerDevices(cd))
	}
	// 使用分号作为分隔符连接所有容器的设备信息
	// 最终格式类似："UUID,Type,Mem,Cores:;UUID,Type,Mem,Cores:;"
	return strings.Join(ss, ";")
}

// decodeContainerDevices 从字符串解析容器的设备分配信息
// 参数 str: 编码的容器设备信息字符串，格式为 "UUID,Type,UsedMem,UsedCores:"
// 返回解析后的ContainerDevices切片
func decodeContainerDevices(str string) ContainerDevices {
	// 检查输入字符串是否为空，如果为空则返回空切片
	if len(str) == 0 {
		return ContainerDevices{}
	}
	// 按冒号分割字符串，获取每个设备的信息
	cd := strings.Split(str, ":")
	// 创建用于存储解析结果的切片
	contdev := ContainerDevices{}
	// 创建临时设备结构体用于存储单个设备信息
	tmpdev := ContainerDevice{}
	// 冗余的空字符串检查（第4行已检查过）
	if len(str) == 0 {
		return contdev
	}
	// 遍历分割后的每个设备信息
	for _, val := range cd {
		// 检查当前设备信息是否包含逗号（完整的设备信息应该有逗号分隔的字段）
		if strings.Contains(val, ",") {
			// 按逗号分割设备属性
			tmpstr := strings.Split(val, ",")
			// 从分割结果中提取UUID（第一个字段）
			tmpdev.UUID = tmpstr[0]
			// 提取设备类型（第二个字段）
			tmpdev.Type = tmpstr[1]
			// 解析已使用的内存量（第三个字段），使用10进制，32位整数
			devmem, _ := strconv.ParseInt(tmpstr[2], 10, 32)
			tmpdev.Usedmem = uint(devmem)
			// 解析已使用的核心百分比（第四个字段），使用10进制，32位整数
			devcores, _ := strconv.ParseInt(tmpstr[3], 10, 32)
			tmpdev.Usedcores = uint(devcores)
			// 将解析完成的设备信息添加到结果切片中
			contdev = append(contdev, tmpdev)
		}
	}
	// 返回解析后的设备信息切片
	return contdev
}

// decodePodDevices 从字符串解析Pod中所有容器的设备分配信息
// 参数 str: 编码的Pod设备信息字符串，多个容器信息用分号分隔
// 返回解析后的ContainerDevices切片的切片，每个元素代表一个容器的设备分配
func decodePodDevices(str string) []ContainerDevices {
	// 检查输入字符串是否为空，如果为空则返回空切片
	if len(str) == 0 {
		return []ContainerDevices{}
	}
	// 创建用于存储解析结果的切片
	var pd []ContainerDevices
	// 按分号分割字符串，获取每个容器的设备信息
	for _, s := range strings.Split(str, ";") {
		// 调用decodeContainerDevices解析单个容器的设备信息
		cd := decodeContainerDevices(s)
		// 将解析结果添加到Pod设备列表中
		pd = append(pd, cd)
	}
	// 返回解析后的Pod设备信息
	return pd
}

// checkVGPUResourcesInPod 检查Pod中是否存在vGPU资源请求
// 参数 pod: 要检查的Pod对象
// 返回布尔值，如果Pod中任一容器请求了vGPU资源则返回true，否则返回false
func checkVGPUResourcesInPod(pod *v1.Pod) bool {
	// 遍历Pod中的所有容器
	for _, container := range pod.Spec.Containers {
		// 检查容器的资源限制中是否存在vGPU内存请求
		// ResourceMemoryName通常为 "volcano.sh/vgpu-memory"
		_, ok := container.Resources.Limits[v1.ResourceName(getConfig().ResourceMemoryName)]
		// 如果存在vGPU内存请求，返回true
		if ok {
			return true
		}
		// 检查容器的资源限制中是否存在vGPU数量请求
		// ResourceCountName通常为 "volcano.sh/vgpu-number"
		_, ok = container.Resources.Limits[v1.ResourceName(getConfig().ResourceCountName)]
		// 如果存在vGPU数量请求，返回true
		if ok {
			return true
		}
	}
	// 如果所有容器都没有vGPU资源请求，返回false
	return false
}

// resourcereqs 解析Pod中每个容器的GPU资源请求，生成设备请求列表
// 参数 pod: 要解析的Pod对象
// 返回每个容器的设备请求信息切片
func resourcereqs(pod *v1.Pod) []ContainerDeviceRequest {
	// 从配置中获取各种vGPU资源的名称
	resourceName := v1.ResourceName(getConfig().ResourceCountName)  // GPU数量资源名，如 "volcano.sh/vgpu-number"
	resourceMem := v1.ResourceName(getConfig().ResourceMemoryName)  // GPU内存资源名，如 "volcano.sh/vgpu-memory"
	resourceMemPercentage := v1.ResourceName(getConfig().ResourceMemoryPercentageName)  // GPU内存百分比资源名
	resourceCores := v1.ResourceName(getConfig().ResourceCoreName)  // GPU核心资源名，如 "volcano.sh/vgpu-cores"
	counts := []ContainerDeviceRequest{}  // 初始化设备请求列表

	// 遍历Pod中的所有容器，统计每个容器的NVIDIA GPU资源需求
	for i := 0; i < len(pod.Spec.Containers); i++ {
		// 标记是否为单设备请求（仅请求内存，没有明确GPU数量）
		singledevice := false
		// 首先检查是否有GPU数量请求
		v, ok := pod.Spec.Containers[i].Resources.Limits[resourceName]
		if !ok {
			// 如果没有GPU数量请求，检查是否有内存请求
			// 如果仅有内存请求，将其视为请求单个GPU
			v, ok = pod.Spec.Containers[i].Resources.Limits[resourceMem]
			singledevice = true
		}
		// 如果找到GPU数量或内存请求，则处理该容器的资源需求
		if ok {
			// 默认请求1个GPU
			n := int64(1)
			// 如果不是单设备模式（即有明确的GPU数量请求），则提取数量值
			if !singledevice {
				n, _ = v.AsInt64()
			}

			// 解析GPU内存请求（以MB为单位）
			memnum := uint(0)
			// 首先检查资源限制中的内存请求
			mem, ok := pod.Spec.Containers[i].Resources.Limits[resourceMem]
			if !ok {
				// 如果限制中没有，检查资源请求中的内存
				mem, ok = pod.Spec.Containers[i].Resources.Requests[resourceMem]
			}
			if ok {
				// 将内存值转换为int64，然后转为uint存储
				memnums, ok := mem.AsInt64()
				if ok {
					memnum = uint(memnums)
				}
			}

			// 解析GPU内存百分比请求
			// 101是特殊值，表示未设置百分比
			mempnum := int32(101)
			// 首先检查资源限制中的内存百分比请求
			mem, ok = pod.Spec.Containers[i].Resources.Limits[resourceMemPercentage]
			if !ok {
				// 如果限制中没有，检查资源请求中的内存百分比
				mem, ok = pod.Spec.Containers[i].Resources.Requests[resourceMemPercentage]
			}
			if ok {
				// 提取百分比值
				mempnums, ok := mem.AsInt64()
				if ok {
					mempnum = int32(mempnums)
				}
			}
			// 如果既没有设置百分比（101）又没有设置绝对内存（0），则默认使用100%
			if mempnum == 101 && memnum == 0 {
				mempnum = 100
			}

			// 解析GPU核心（计算能力）请求（以百分比表示）
			corenum := uint(0)
			// 首先检查资源限制中的核心请求
			core, ok := pod.Spec.Containers[i].Resources.Limits[resourceCores]
			if !ok {
				// 如果限制中没有，检查资源请求中的核心
				core, ok = pod.Spec.Containers[i].Resources.Requests[resourceCores]
			}
			if ok {
				// 提取核心百分比值
				corenums, ok := core.AsInt64()
				if ok {
					corenum = uint(corenums)
				}
			}

			// 构造容器的设备请求结构体并添加到列表中
			counts = append(counts, ContainerDeviceRequest{
				Nums:             int32(n),      // 请求的GPU数量
				Type:             "NVIDIA",      // 设备类型固定为NVIDIA
				Memreq:           memnum,        // 请求的GPU内存量（MB）
				MemPercentagereq: int32(mempnum), // 请求的GPU内存百分比
				Coresreq:         corenum,       // 请求的GPU核心百分比
			})
		}
	}
	// 记录所有容器的资源请求到日志（日志级别3）
	klog.V(3).Infoln("counts=", counts)
	// 返回所有容器的设备请求列表
	return counts
}

// checkGPUtype 检查GPU卡类型是否符合Pod注解中的GPU类型限制
// 参数 annos: Pod的注解映射
// 参数 cardtype: 节点上GPU卡的类型字符串
// 返回布尔值，表示该类型的GPU是否可以被使用
func checkGPUtype(annos map[string]string, cardtype string) bool {
	// 首先检查是否有GPU类型白名单（只使用指定类型的GPU）
	inuse, ok := annos[GPUInUse]
	if ok {
		// 如果白名单中只有一个GPU类型（没有逗号分隔）
		if !strings.Contains(inuse, ",") {
			// 检查节点上的GPU类型是否包含白名单中的类型（不区分大小写）
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(inuse)) {
				return true  // GPU类型匹配，可以使用
			}
		} else {
			// 如果白名单中有多个GPU类型（逗号分隔的列表）
			for _, val := range strings.Split(inuse, ",") {
				// 检查节点上的GPU类型是否包含白名单中的任一类型
				if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(val)) {
					return true  // GPU类型匹配，可以使用
				}
			}
		}
		// 如果遍历完白名单中所有类型都不匹配，则不能使用该GPU
		return false
	}

	// 其次检查是否有GPU类型黑名单（不使用指定类型的GPU）
	nouse, ok := annos[GPUNoUse]
	if ok {
		// 如果黑名单中只有一个GPU类型（没有逗号分隔）
		if !strings.Contains(nouse, ",") {
			// 检查节点上的GPU类型是否包含黑名单中的类型
			if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(nouse)) {
				return false  // GPU类型在黑名单中，不能使用
			}
		} else {
			// 如果黑名单中有多个GPU类型（逗号分隔的列表）
			for _, val := range strings.Split(nouse, ",") {
				// 检查节点上的GPU类型是否包含黑名单中的任一类型
				if strings.Contains(strings.ToUpper(cardtype), strings.ToUpper(val)) {
					return false  // GPU类型在黑名单中，不能使用
				}
			}
		}
		// 如果遍历完黑名单中所有类型都不匹配，则可以使用该GPU
		return true
	}

	// 如果既没有白名单也没有黑名单，默认可以使用任何类型的GPU
	return true
}

// checkType 检查设备类型是否与容器请求的类型匹配
// 参数 annos: Pod的注解映射
// 参数 d: 节点上的GPU设备
// 参数 n: 容器的设备请求
// 返回布尔值，表示该设备类型是否匹配容器的请求
func checkType(annos map[string]string, d GPUDevice, n ContainerDeviceRequest) bool {
	// 首先进行通用类型检查，设备类型应包含请求的类型
	// 例如，设备类型为"NVIDIA-A100"，请求类型为"NVIDIA"
	// 或设备类型为"MLU-370"，请求类型为"MLU"
	if !strings.Contains(d.Type, n.Type) {
		return false  // 基本类型不匹配，无法使用该设备
	}

	// 如果请求的是NVIDIA设备，进一步检查具体型号是否符合Pod注解中的限制
	if n.Type == NvidiaGPUDevice {
		return checkGPUtype(annos, d.Type)  // 调用checkGPUtype检查是否符合类型限制
	}

	// 如果设备类型不是NVIDIA，记录错误并拒绝使用
	// 这表明当前只支持NVIDIA设备，其他设备类型被视为不可识别
	klog.Errorf("Unrecognized device %v", n.Type)
	return false  // 不可识别的设备类型，无法使用
}

// getGPUDeviceSnapShot 创建GPU设备状态的快照
// 注意：这不是严格意义上的深拷贝，某些指针项仍然指向原始对象
// 参数 snap: 原始的GPU设备状态
// 返回创建的设备状态快照
func getGPUDeviceSnapShot(snap *GPUDevices) *GPUDevices {
	// 创建新的GPUDevices结构体
	ret := GPUDevices{
		Name:    snap.Name,            // 复制节点名称
		Device:  make(map[int]*GPUDevice), // 创建新的设备映射
		Score:   float64(0),           // 重置得分为0
		Sharing: snap.Sharing,         // 复制共享策略（浅拷贝，共享同一对象）
	}
	// 遍历原始设备映射中的每个设备
	for index, val := range snap.Device {
		// 确保设备不为nil
		if val != nil {
			// 为每个设备创建新的GPUDevice结构体
			ret.Device[index] = &GPUDevice{
				ID:          val.ID,        // 复制设备ID
				Node:        val.Node,      // 复制节点名称
				UUID:        val.UUID,      // 复制设备UUID
				PodMap:      val.PodMap,    // 复制Pod映射（浅拷贝，共享同一对象）
				Memory:      val.Memory,    // 复制内存容量
				Number:      val.Number,    // 复制共享实例数量
				Type:        val.Type,      // 复制设备类型
				Health:      val.Health,    // 复制健康状态
				UsedNum:     val.UsedNum,   // 复制已使用实例数量
				UsedMem:     val.UsedMem,   // 复制已使用内存
				UsedCore:    val.UsedCore,  // 复制已使用核心百分比
				MigTemplate: val.MigTemplate, // 复制MIG模板（浅拷贝，共享同一对象）
				MigUsage:    val.MigUsage,  // 复制MIG使用情况（会在下一行被深拷贝替换）
			}
			// 对MigUsage进行深拷贝，确保修改快照不会影响原始对象的MIG配置
			ret.Device[index].MigUsage = deepCopyMigInUse(val.MigUsage)
			// 记录设备快照的创建过程到日志（日志级别4）
			klog.V(4).Infoln("getGPUDeviceSnapShot:", ret.Device[index].UsedMem, val.UsedMem, ret.Device[index].MigUsage, val.MigUsage)
		}
	}
	// 返回创建的设备状态快照
	return &ret
}

// deepCopyMigInUse 深拷贝MIG使用情况结构体
// 参数 src: 原始的MIG使用情况
// 返回深拷贝后的MIG使用情况
func deepCopyMigInUse(src config.MigInUse) config.MigInUse {
	// 创建新的MigInUse结构体，复制Index字段
	dst := config.MigInUse{
		Index: src.Index,  // 复制当前正在使用的MIG几何配置索引
	}

	// 为UsageList创建新的切片，容量与原始切片相同
	dst.UsageList = make(config.MIGS, len(src.UsageList))
	// 遍历源UsageList，逐个复制MIG模板使用情况
	for i, usage := range src.UsageList {
		// 为每个MIG模板使用情况创建新的结构体
		dst.UsageList[i] = config.MigTemplateUsage{
			Name:      usage.Name,    // 复制MIG模板名称
			Memory:    usage.Memory,  // 复制MIG实例的内存大小
			InUse:     usage.InUse,   // 复制是否正在使用的标志
			UsedIndex: make([]int, len(usage.UsedIndex)), // 为已使用索引创建新切片
		}
		// 深拷贝已使用索引切片，避免共享底层数组
		copy(dst.UsageList[i].UsedIndex, usage.UsedIndex)
	}

	// 返回深拷贝后的MIG使用情况
	return dst
}

// getSharingMode 解析并返回GPU共享模式
// 默认情况下，使用HAMI-core作为GPU分区模式
// 参数 mode: 模式字符串
// 返回规范化的GPU共享模式常量
func getSharingMode(mode string) string {
	switch mode {
	case vGPUControllerMIG:
		// 如果模式为MIG（Multi-Instance GPU），返回MIG常量
		// MIG是NVIDIA提供的硬件级GPU分区技术
		return vGPUControllerMIG
	default:
		// 默认情况下返回HAMI-core模式
		// HAMI-core是基于软件的vGPU实现，使用CUDA API劫持技术
		return vGPUControllerHAMICore
	}
}

/**
 * my 
 * func checkNodeGPUSharingPredicateAndScore(
    pod *v1.Pod,                // 要调度的 Pod
    gssnap *GPUDevices,         // 节点上的 GPU 设备快照
    replicate bool,             // 是否创建设备状态副本（用于模拟分配）
    schedulePolicy string       // 调度策略（binpack/spread）
	) (
		bool,                       // 是否可以调度
		[]ContainerDevices,         // 容器设备分配结果
		float64,                    // 节点得分
		error                       // 错误信息
	)
 */
// checkNodeGPUSharingPredicate checks if a pod with gpu requirement can be scheduled on a node.
func checkNodeGPUSharingPredicateAndScore(pod *v1.Pod, gssnap *GPUDevices, replicate bool, schedulePolicy string) (bool, []ContainerDevices, float64, error) {
	// no gpu sharing request
	score := float64(0)
	if !checkVGPUResourcesInPod(pod) {
		return true, []ContainerDevices{}, 0, nil
	}

	// 从 Pod 注解中获取 volcano.sh/vgpu-mode 值，确定 Pod 请求的 GPU 共享模式
	// 如果 Pod 指定共享模式(hami-core 或 mig)，需要与节点的 GPU 模式一致
	// 如果 Pod 没有指定共享模式，则任何节点 GPU 模式都被视为兼容

	// if the pod specify the sharing mode but the device is not in the same mode, return not fitted;
	// if the pod does not speficy the sharing mode, any device mode will be fitted
	podSharingMode, ok := pod.Annotations[GPUModeAnnotation]
	if ok && podSharingMode != gssnap.Mode {
		return false, []ContainerDevices{}, 0, fmt.Errorf("pod required sharing mode %s is not the same as the node mode %s", podSharingMode, gssnap.Mode)
	}

	// 解析 Pod 容器的资源限制，提取 vGPU 数量、内存、百分比和核心请求
	ctrReq := resourcereqs(pod)
	if len(ctrReq) == 0 {
		return true, []ContainerDevices{}, 0, nil
	}

	var gs *GPUDevices
	if replicate {  // 创建设备快照，避免修改原始状态，只用于检查和打分
		gs = getGPUDeviceSnapShot(gssnap)
	} else { // 实际分配设备，直接使用原始设备状态
		gs = gssnap
	}

	// 遍历每个容器的 vGPU 资源请求，为每个容器分配 vGPU 资源
	ctrdevs := []ContainerDevices{}
	for _, val := range ctrReq {
		devs := []ContainerDevice{}
		// 如果请求的 vGPU 卡数量超过节点上实际可用的 vGPU 卡数量，返回不可调度状态和相应错误
		if int(val.Nums) > len(gs.Device) {
			return false, []ContainerDevices{}, 0, fmt.Errorf("no enough gpu cards on node %s", gs.Name)
		}
		klog.V(3).InfoS("Allocating device for container", "request", val)
		// 倒序遍历可能是为优先考虑某些特定设备，如索引较高的可能是性能更好的设备
		for i := len(gs.Device) - 1; i >= 0; i-- {
			klog.V(3).InfoS("Scoring pod request", "memReq", val.Memreq, "memPercentageReq", val.MemPercentagereq, "coresReq", val.Coresreq, "Nums", val.Nums, "Index", i, "ID", gs.Device[i].ID)
			klog.V(3).InfoS("Current Device", "Index", i, "TotalMemory", gs.Device[i].Memory, "UsedMemory", gs.Device[i].UsedMem, "UsedCores", gs.Device[i].UsedCore, "replicate", replicate)
		    // 如果当前 vGPU 的可共享实例数量小于等于已使用实例数量，跳过
			if gs.Device[i].Number <= uint(gs.Device[i].UsedNum) {
				continue
			}
			
			memreqForCard := uint(0)
			// if we have mempercentage request, we ignore the mem request for every cards
			
			// 如果指定内存百分比请求(MemPercentagereq不等于特殊值101)，计算对应的绝对内存值
			if val.MemPercentagereq != 101 {
				memreqForCard = uint(float64(gs.Device[i].Memory) * float64(val.MemPercentagereq) / 100.0)
			} else { // 如果没有百分比请求，直接使用指定的内存请求量 Memreq
				memreqForCard = val.Memreq
			}
			// 如果设备剩余内存小于请求的内存量，跳过
			if int(gs.Device[i].Memory)-int(gs.Device[i].UsedMem) < int(memreqForCard) {
				continue
			}

			// 如果设备已用核心百分比加请求的核心百分比超过100%，跳过
			if gs.Device[i].UsedCore+val.Coresreq > 100 {
				continue
			}
			// 如果请求 100% 的核心，且设备已有其他使用者(UsedNum > 0)，跳过
			// Coresreq=100 indicates it want this card exclusively
			if val.Coresreq == 100 && gs.Device[i].UsedNum > 0 {
				continue
			}
			// 如果设备的核心已完全使用(UsedCore=100)，不允许分配核心请求为 0 的任务
			// You can't allocate core=0 job to an already full GPU
			if gs.Device[i].UsedCore == 100 && val.Coresreq == 0 {
				continue
			}
			// 根据 Pod 注解检查类型兼容性，用于 GPU Type 和 GPU UUID 的精准调度能力
			if !checkType(pod.Annotations, *gs.Device[i], val) {
				klog.Errorln("failed checktype", gs.Device[i].Type, val.Type)
				continue
			}

			// 调用共享模式处理器的 TryAddPod 方法，尝试将 Pod 添加到当前设备
			// 根据节点的共享模式(hami-core或mig)，使用对应的处理器实现不同的逻辑
			fit, uuid := gs.Sharing.TryAddPod(gs.Device[i], memreqForCard, uint(val.Coresreq))
			if !fit {
				klog.V(3).Info(gs.Device[i].ID, "not fit")
				continue
			}

			//total += gs.Devices[i].Count
			//free += node.Devices[i].Count - node.Devices[i].Used
			// 减少剩余需要分配的GPU数量，记录信息并根据调度策略(binpack或spread)计算设备得分，累加到总得分中
			if val.Nums > 0 {
				val.Nums--
				klog.V(3).Info("fitted uuid: ", uuid)
				devs = append(devs, ContainerDevice{
					UUID:      uuid,
					Type:      val.Type,
					Usedmem:   memreqForCard,
					Usedcores: val.Coresreq,
				})
				score += GPUScore(schedulePolicy, gs.Device[i])
			}
			// 如果已分配足够的GPU设备(val.Nums降至0)，退出当前设备遍历循环
			if val.Nums == 0 {
				break
			}
		}
		// 检查遍历完所有设备后，是否满足容器的所有GPU请求
		if val.Nums > 0 {
			return false, []ContainerDevices{}, 0, fmt.Errorf("not enough gpu fitted on this node")
		}
		ctrdevs = append(ctrdevs, devs)
	}
	// 当所有容器的GPU请求都得到满足后，返回可调度状态、设备分配结果、累计得分和nil错误
	// 表示Pod可以被成功调度到该节点，并给出具体的设备分配方案
	return true, ctrdevs, score, nil
}

// GPUScore 根据调度策略计算GPU设备的得分
// 参数 schedulePolicy: 调度策略，支持"binpack"（集中）和"spread"（分散）
// 参数 device: 要计算得分的GPU设备
// 返回根据策略计算的设备得分，得分越高表示该设备越适合被选中
func GPUScore(schedulePolicy string, device *GPUDevice) float64 {
	// 定义得分变量
	var score float64
	// 根据不同的调度策略计算得分
	switch schedulePolicy {
	case binpackPolicy:
		// binpack策略：根据内存利用率计算得分，利用率越高得分越高
		// 倾向于选择已经有较高利用率的设备，尽可能将任务集中到少数几个GPU上
		// 这样可以减少碎片化，提高资源利用效率
		score = binpackMultiplier * (float64(device.UsedMem) / float64(device.Memory))
	case spreadPolicy:
		// spread策略：仅有一个使用者的设备获得最高分
		// 促使调度器选择使用较少的设备，尽可能将任务分散到不同的GPU上
		// 这样可以减少单个GPU的负载，避免资源竞争
		if device.UsedNum == 1 {
			score = spreadMultiplier
		}
	default:
		// 未识别的策略，返回0分
		score = float64(0)
	}
	// 返回计算的得分
	return score
}

// patchPodAnnotations 更新Pod的注解信息
// 参数 kubeClient: Kubernetes客户端接口
// 参数 pod: 要更新注解的Pod对象
// 参数 annotations: 要添加或更新的注解映射
// 返回操作过程中可能出现的错误
func patchPodAnnotations(kubeClient kubernetes.Interface, pod *v1.Pod, annotations map[string]string) error {
	// 定义用于补丁操作的元数据结构体
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`  // 注解映射，omitempty表示如果为空则不包含在JSON中
	}
	// 定义用于补丁操作的Pod结构体
	type patchPod struct {
		Metadata patchMetadata `json:"metadata"`  // 元数据字段
		//Spec     patchSpec     `json:"spec,omitempty"`  // 注释掉的规格字段
	}

	// 创建补丁Pod对象并设置注解
	p := patchPod{}
	p.Metadata.Annotations = annotations

	// 将补丁对象序列化为JSON字节
	bytes, err := json.Marshal(p)
	if err != nil {
		// 序列化失败，返回错误
		return err
	}
	// 使用Kubernetes客户端执行Pod补丁操作
	// 使用StrategicMergePatchType类型进行补丁合并
	_, err = kubeClient.CoreV1().Pods(pod.Namespace).
		Patch(context.Background(), pod.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		// 补丁操作失败，记录错误日志
		klog.Errorf("patch pod %v failed, %v", pod.Name, err)
	}

	// 返回操作结果（成功或错误）
	return err
}

// patchNodeAnnotations 更新节点的注解信息
// 参数 node: 要更新注解的节点对象
// 参数 annotations: 要添加或更新的注解映射
// 返回操作过程中可能出现的错误
func patchNodeAnnotations(node *v1.Node, annotations map[string]string) error {
	// 定义用于补丁操作的元数据结构体
	type patchMetadata struct {
		Annotations map[string]string `json:"annotations,omitempty"`  // 注解映射，omitempty表示如果为空则不包含在JSON中
	}
	// 定义用于补丁操作的节点结构体
	type patchNode struct {
		Metadata patchMetadata `json:"metadata"`  // 元数据字段
		//Spec     patchSpec     `json:"spec,omitempty"`  // 注释掉的规格字段
	}

	// 创建补丁节点对象并设置注解
	p := patchNode{}
	p.Metadata.Annotations = annotations

	// 将补丁对象序列化为JSON字节
	bytes, err := json.Marshal(p)
	if err != nil {
		// 序列化失败，返回错误
		return err
	}
	// 使用设备模块中的Kubernetes客户端执行节点补丁操作
	// 使用StrategicMergePatchType类型进行补丁合并
	_, err = devices.GetClient().CoreV1().Nodes().
		Patch(context.Background(), node.Name, k8stypes.StrategicMergePatchType, bytes, metav1.PatchOptions{})
	if err != nil {
		// 补丁操作失败，记录错误日志
		klog.Errorf("patch node %v failed, %v", node.Name, err)
	}
	// 返回操作结果（成功或错误）
	return err
}

// getConfig 获取NVIDIA相关的配置信息
// 返回NvidiaConfig结构体，包含NVIDIA设备相关的配置参数
func getConfig() config.NvidiaConfig {
	// 检查全局配置是否存在
	if config.GetConfig() != nil {
		// 如果全局配置存在，返回其中的NVIDIA配置部分
		return config.GetConfig().NvidiaConfig
	}
	// 如果全局配置不存在，返回默认设备配置中的NVIDIA配置
	return config.GetDefaultDevicesConfig().NvidiaConfig
}
