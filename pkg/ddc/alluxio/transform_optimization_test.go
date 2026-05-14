/*
Copyright 2020 The Fluid Authors.

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

package alluxio

import (
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	datav1alpha1 "github.com/fluid-cloudnative/fluid/api/v1alpha1"
	"github.com/fluid-cloudnative/fluid/pkg/common"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Constants for test values used multiple times
const (
	testJnifuseKey           = "alluxio.fuse.jnifuse.enabled"
	testLibfuseVersionKey    = "alluxio.fuse.jnifuse.libfuse.version"
	testS3ThreadsMaxKey      = "alluxio.underfs.s3.threads.max"
	testWorkerClientPoolKey  = "alluxio.user.block.worker.client.pool.max"
	testBlockSizeKey         = "alluxio.user.block.size.bytes.default"
	testStreamingChunkKey    = "alluxio.user.streaming.reader.chunk.size.bytes"
	testLocalChunkKey        = "alluxio.user.local.reader.chunk.size.bytes"
	testWorkerReaderKey      = "alluxio.worker.network.reader.buffer.size"
	testDirectMemoryIOKey    = "alluxio.user.direct.memory.io.enabled"
	testMasterRpcPortKey     = "alluxio.master.rpc.port"
	testMasterWebPortKey     = "alluxio.master.web.port"
	testWorkerRpcPortKey     = "alluxio.worker.rpc.port"
	testWorkerWebPortKey     = "alluxio.worker.web.port"
	testJobMasterRpcPortKey  = "alluxio.job.master.rpc.port"
	testJobMasterWebPortKey  = "alluxio.job.master.web.port"
	testJobWorkerRpcPortKey  = "alluxio.job.worker.rpc.port"
	testJobWorkerWebPortKey  = "alluxio.job.worker.web.port"
	testJobWorkerDataPortKey = "alluxio.job.worker.data.port"
	testMountPath            = "/mnt/runtime"
	testExpectedBlockSize    = "256MB"
	testExpectedJnifuseTrue  = "true"
	testExpectedJnifuseFalse = "false"
)

// Default JVM options for fuse, shared across multiple tests
var expectedDefaultJvmOptions = []string{
	"-Xmx16G",
	"-Xms16G",
	"-XX:+UseG1GC",
	"-XX:MaxDirectMemorySize=32g",
	"-XX:+UnlockExperimentalVMOptions",
}

var _ = Describe("AlluxioEngine Transform Optimization Tests", Label("pkg.ddc.alluxio.transform_optimization_test.go"), func() {
	var engine *AlluxioEngine

	BeforeEach(func() {
		engine = &AlluxioEngine{}
	})

	Describe("optimizeDefaultProperties", func() {
		Context("when no properties are set in runtime", func() {
			It("should set default jnifuse property to true", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{},
					},
				}
				alluxioValue := &Alluxio{}

				engine.optimizeDefaultProperties(runtime, alluxioValue)

				Expect(alluxioValue.Properties[testJnifuseKey]).To(Equal(testExpectedJnifuseTrue))
			})
		})

		Context("when jnifuse property is already set to false", func() {
			It("should preserve the existing value", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{
							testJnifuseKey: testExpectedJnifuseFalse,
						},
					},
				}
				alluxioValue := &Alluxio{}

				engine.optimizeDefaultProperties(runtime, alluxioValue)

				Expect(alluxioValue.Properties[testJnifuseKey]).To(Equal(testExpectedJnifuseFalse))
			})
		})
	})

	Describe("optimizeDefaultPropertiesAndFuseForHTTP", func() {
		Context("when dataset has HTTPS mount point", func() {
			It("should set block size property for HTTP", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
					Fuse: Fuse{
						Args: []string{"fuse", "--fuse-opts=kernel_cache,rw,max_read=131072,attr_timeout=7200,entry_timeout=7200,nonempty"},
					},
				}
				dataset := &datav1alpha1.Dataset{
					Spec: datav1alpha1.DatasetSpec{
						Mounts: []datav1alpha1.Mount{
							{MountPoint: "https://mirrors.bit.edu.cn/apache/zookeeper/zookeeper-3.6.2/"},
						},
					},
				}

				engine.optimizeDefaultProperties(runtime, alluxioValue)
				engine.optimizeDefaultPropertiesAndFuseForHTTP(runtime, dataset, alluxioValue)

				Expect(alluxioValue.Properties[testBlockSizeKey]).To(Equal(testExpectedBlockSize))
			})
		})
	})

	Describe("optimizeDefaultPropertiesForHighConcurrencyS3", func() {
		Context("when the high concurrency profile is enabled on an S3 dataset", func() {
			It("should set the JNR/libfuse2 and S3 concurrency defaults", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							alluxioS3HighConcurrencyProfileAnnotation: "true",
						},
					},
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{},
					},
				}
				dataset := &datav1alpha1.Dataset{
					Spec: datav1alpha1.DatasetSpec{
						Mounts: []datav1alpha1.Mount{
							{MountPoint: "s3://bucket/path"},
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
				}
				userProperties := copyAlluxioProperties(runtime.Spec.Properties)

				engine.optimizeDefaultProperties(runtime, alluxioValue)
				engine.optimizeDefaultPropertiesForHighConcurrencyS3(runtime, dataset, alluxioValue, userProperties)

				Expect(alluxioValue.Properties[testJnifuseKey]).To(Equal(testExpectedJnifuseFalse))
				Expect(alluxioValue.Properties[testLibfuseVersionKey]).To(Equal("2"))
				Expect(alluxioValue.Properties[testS3ThreadsMaxKey]).To(Equal("2048"))
				Expect(alluxioValue.Properties[testWorkerClientPoolKey]).To(Equal("8192"))
				Expect(alluxioValue.Properties[testBlockSizeKey]).To(Equal("64MB"))
				Expect(alluxioValue.Properties[testStreamingChunkKey]).To(Equal("64MB"))
				Expect(alluxioValue.Properties[testLocalChunkKey]).To(Equal("64MB"))
				Expect(alluxioValue.Properties[testWorkerReaderKey]).To(Equal("64MB"))
				Expect(alluxioValue.Properties[testDirectMemoryIOKey]).To(Equal("false"))
			})
		})

		Context("when value properties share the runtime properties map", func() {
			It("should still take precedence over generic defaults when called after them", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							alluxioS3HighConcurrencyProfileAnnotation: "true",
						},
					},
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{
							"alluxio.user.file.writetype.default": "CACHE_THROUGH",
						},
					},
				}
				dataset := &datav1alpha1.Dataset{
					Spec: datav1alpha1.DatasetSpec{
						Mounts: []datav1alpha1.Mount{
							{MountPoint: "s3://bucket/path"},
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: runtime.Spec.Properties,
				}
				userProperties := copyAlluxioProperties(runtime.Spec.Properties)

				engine.optimizeDefaultProperties(runtime, alluxioValue)
				engine.optimizeDefaultPropertiesForHighConcurrencyS3(runtime, dataset, alluxioValue, userProperties)

				Expect(alluxioValue.Properties[testJnifuseKey]).To(Equal(testExpectedJnifuseFalse))
				Expect(alluxioValue.Properties[testLibfuseVersionKey]).To(Equal("2"))
				Expect(alluxioValue.Properties[testBlockSizeKey]).To(Equal("64MB"))
				Expect(alluxioValue.Properties[testDirectMemoryIOKey]).To(Equal("false"))
			})
		})

		Context("when users override profile properties", func() {
			It("should preserve the user supplied values", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							alluxioS3HighConcurrencyProfileAnnotation: "true",
						},
					},
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{
							testS3ThreadsMaxKey:   "512",
							testBlockSizeKey:      "128MB",
							testDirectMemoryIOKey: "true",
						},
					},
				}
				dataset := &datav1alpha1.Dataset{
					Spec: datav1alpha1.DatasetSpec{
						Mounts: []datav1alpha1.Mount{
							{MountPoint: "s3://bucket/path"},
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{
						testS3ThreadsMaxKey:   "512",
						testBlockSizeKey:      "128MB",
						testDirectMemoryIOKey: "true",
					},
				}
				userProperties := copyAlluxioProperties(runtime.Spec.Properties)

				engine.optimizeDefaultProperties(runtime, alluxioValue)
				engine.optimizeDefaultPropertiesForHighConcurrencyS3(runtime, dataset, alluxioValue, userProperties)

				Expect(alluxioValue.Properties[testS3ThreadsMaxKey]).To(Equal("512"))
				Expect(alluxioValue.Properties[testBlockSizeKey]).To(Equal("128MB"))
				Expect(alluxioValue.Properties[testDirectMemoryIOKey]).To(Equal("true"))
			})
		})

		Context("when the profile is enabled on a non-S3 dataset", func() {
			It("should not change defaults", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							alluxioS3HighConcurrencyProfileAnnotation: "true",
						},
					},
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{},
					},
				}
				dataset := &datav1alpha1.Dataset{
					Spec: datav1alpha1.DatasetSpec{
						Mounts: []datav1alpha1.Mount{
							{MountPoint: "https://example.com/data"},
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
				}
				userProperties := copyAlluxioProperties(runtime.Spec.Properties)

				engine.optimizeDefaultProperties(runtime, alluxioValue)
				engine.optimizeDefaultPropertiesForHighConcurrencyS3(runtime, dataset, alluxioValue, userProperties)

				Expect(alluxioValue.Properties[testJnifuseKey]).To(Equal(testExpectedJnifuseTrue))
				Expect(alluxioValue.Properties[testBlockSizeKey]).To(Equal("16MB"))
			})
		})
	})

	Describe("normalizeFuseArgsForLibfuseVersion", func() {
		Context("when libfuse2 is selected", func() {
			It("should remove libfuse3-only max_idle_threads options", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{
							testLibfuseVersionKey: "2",
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
					Fuse: Fuse{
						Args: []string{"fuse", "--fuse-opts=kernel_cache,rw,max_idle_threads=256,max_background=256"},
					},
				}

				normalizeFuseArgsForLibfuseVersion(runtime, alluxioValue)

				Expect(alluxioValue.Fuse.Args).To(Equal([]string{"fuse", "--fuse-opts=kernel_cache,rw,max_background=256"}))
			})
		})

		Context("when libfuse3 is selected", func() {
			It("should preserve max_idle_threads options", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{
							testLibfuseVersionKey: "3",
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
					Fuse: Fuse{
						Args: []string{"fuse", "--fuse-opts=kernel_cache,rw,max_idle_threads=256"},
					},
				}

				normalizeFuseArgsForLibfuseVersion(runtime, alluxioValue)

				Expect(alluxioValue.Fuse.Args).To(Equal([]string{"fuse", "--fuse-opts=kernel_cache,rw,max_idle_threads=256"}))
			})
		})
	})

	Describe("setDefaultProperties", func() {
		Context("when property is not set in runtime", func() {
			It("should set the default value in alluxioValue", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
				}

				setDefaultProperties(runtime, alluxioValue, testJnifuseKey, testExpectedJnifuseTrue)

				Expect(alluxioValue.Properties[testJnifuseKey]).To(Equal(testExpectedJnifuseTrue))
			})
		})

		Context("when property is already set in runtime", func() {
			It("should NOT set the default value in alluxioValue (leave it empty)", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Properties: map[string]string{
							testJnifuseKey: testExpectedJnifuseFalse,
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
				}

				setDefaultProperties(runtime, alluxioValue, testJnifuseKey, testExpectedJnifuseTrue)

				// When property exists in runtime, setDefaultProperties does NOT set it in alluxioValue
				Expect(alluxioValue.Properties[testJnifuseKey]).To(BeEmpty())
			})
		})
	})

	Describe("optimizeDefaultForMaster", func() {
		Context("when no JVM options are set in runtime", func() {
			It("should set default JVM options including UnlockExperimentalVMOptions", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
				}

				engine.optimizeDefaultForMaster(runtime, alluxioValue)

				Expect(alluxioValue.Master.JvmOptions).To(HaveLen(2))
				Expect(alluxioValue.Master.JvmOptions[1]).To(Equal("-XX:+UnlockExperimentalVMOptions"))
			})
		})

		Context("when JVM options are specified in runtime", func() {
			It("should use the runtime JVM options", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Master: datav1alpha1.AlluxioCompTemplateSpec{
							JvmOptions: []string{"-Xmx4G"},
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
					Master:     Master{},
				}

				engine.optimizeDefaultForMaster(runtime, alluxioValue)

				Expect(alluxioValue.Master.JvmOptions).To(HaveLen(1))
				Expect(alluxioValue.Master.JvmOptions[0]).To(Equal("-Xmx4G"))
			})
		})
	})

	Describe("optimizeDefaultForWorker", func() {
		Context("when no JVM options are set in runtime", func() {
			It("should set default JVM options including UnlockExperimentalVMOptions", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
				}

				engine.optimizeDefaultForWorker(runtime, alluxioValue)

				Expect(alluxioValue.Worker.JvmOptions).To(HaveLen(3))
				Expect(alluxioValue.Worker.JvmOptions[1]).To(Equal("-XX:+UnlockExperimentalVMOptions"))
			})
		})

		Context("when JVM options are specified in runtime", func() {
			It("should use the runtime JVM options", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Worker: datav1alpha1.AlluxioCompTemplateSpec{
							JvmOptions: []string{"-Xmx4G"},
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
				}

				engine.optimizeDefaultForWorker(runtime, alluxioValue)

				Expect(alluxioValue.Worker.JvmOptions).To(HaveLen(1))
				Expect(alluxioValue.Worker.JvmOptions[0]).To(Equal("-Xmx4G"))
			})
		})
	})

	Describe("optimizeDefaultFuse", func() {
		Context("when no JVM options are set with new fuse arg version", func() {
			It("should set default JVM options and append mount path to args", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
					Fuse: Fuse{
						MountPath: testMountPath,
					},
				}
				isNewFuseArgVersion := true

				engine.optimizeDefaultFuse(runtime, alluxioValue, isNewFuseArgVersion)

				expectedArgs := []string{"fuse", "--fuse-opts=kernel_cache,rw", testMountPath, "/"}

				Expect(alluxioValue.Fuse.JvmOptions).To(Equal(expectedDefaultJvmOptions))
				Expect(alluxioValue.Fuse.Args).To(Equal(expectedArgs))
			})
		})

		Context("when no JVM options are set with old fuse arg version", func() {
			It("should set default JVM options without mount path in args", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
					Fuse: Fuse{
						MountPath: testMountPath,
					},
				}
				isNewFuseArgVersion := false

				engine.optimizeDefaultFuse(runtime, alluxioValue, isNewFuseArgVersion)

				expectedArgs := []string{"fuse", "--fuse-opts=kernel_cache,rw"}

				Expect(alluxioValue.Fuse.JvmOptions).To(Equal(expectedDefaultJvmOptions))
				Expect(alluxioValue.Fuse.Args).To(Equal(expectedArgs))
			})
		})

		Context("when JVM options are specified in runtime", func() {
			It("should use the runtime JVM options", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Fuse: datav1alpha1.AlluxioFuseSpec{
							JvmOptions: []string{"-Xmx4G"},
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
				}
				isNewFuseArgVersion := true

				engine.optimizeDefaultFuse(runtime, alluxioValue, isNewFuseArgVersion)

				Expect(alluxioValue.Fuse.JvmOptions).To(HaveLen(1))
				Expect(alluxioValue.Fuse.JvmOptions[0]).To(Equal("-Xmx4G"))
			})
		})

		Context("when fuse args are specified with new fuse arg version", func() {
			It("should append mount path and root to args", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Fuse: datav1alpha1.AlluxioFuseSpec{
							Args: []string{"fuse", "--fuse-opts=kernel_cache,rw,max_read=131072"},
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
					Fuse: Fuse{
						MountPath: testMountPath,
					},
				}
				isNewFuseArgVersion := true

				engine.optimizeDefaultFuse(runtime, alluxioValue, isNewFuseArgVersion)

				expectedArgs := []string{"fuse", "--fuse-opts=kernel_cache,rw,max_read=131072", testMountPath, "/"}
				Expect(alluxioValue.Fuse.Args).To(Equal(expectedArgs))
			})
		})

		Context("when fuse args are specified with old fuse arg version", func() {
			It("should not append mount path to args", func() {
				runtime := &datav1alpha1.AlluxioRuntime{
					Spec: datav1alpha1.AlluxioRuntimeSpec{
						Fuse: datav1alpha1.AlluxioFuseSpec{
							Args: []string{"fuse", "--fuse-opts=kernel_cache,rw,max_read=131072"},
						},
					},
				}
				alluxioValue := &Alluxio{
					Properties: map[string]string{},
				}
				isNewFuseArgVersion := false

				engine.optimizeDefaultFuse(runtime, alluxioValue, isNewFuseArgVersion)

				expectedArgs := []string{"fuse", "--fuse-opts=kernel_cache,rw,max_read=131072"}
				Expect(alluxioValue.Fuse.Args).To(Equal(expectedArgs))
			})
		})
	})

	Describe("setPortProperties", func() {
		Context("when port values are specified in alluxioValue", func() {
			It("should set all port properties correctly", func() {
				port := 20000
				runtime := &datav1alpha1.AlluxioRuntime{}
				alluxioValue := &Alluxio{
					Master: Master{
						Ports: Ports{
							Rpc:      port,
							Web:      port,
							Embedded: 0,
						},
					},
					Worker: Worker{
						Ports: Ports{
							Rpc: port,
							Web: port,
						},
					},
					JobMaster: JobMaster{
						Ports: Ports{
							Rpc:      port,
							Web:      port,
							Embedded: 0,
						},
						Resources: common.Resources{
							Requests: common.ResourceList{
								corev1.ResourceCPU:    "100m",
								corev1.ResourceMemory: "100Mi",
							},
						},
					},
					JobWorker: JobWorker{
						Ports: Ports{
							Rpc:  port,
							Web:  port,
							Data: port,
						},
						Resources: common.Resources{
							Requests: common.ResourceList{
								corev1.ResourceCPU:    "100m",
								corev1.ResourceMemory: "100Mi",
							},
						},
					},
					Properties: map[string]string{},
				}

				testEngine := &AlluxioEngine{
					runtime: runtime,
				}
				testEngine.setPortProperties(runtime, alluxioValue)

				expectedPort := strconv.Itoa(port)
				Expect(alluxioValue.Properties[testMasterRpcPortKey]).To(Equal(expectedPort))
				Expect(alluxioValue.Properties[testMasterWebPortKey]).To(Equal(expectedPort))
				Expect(alluxioValue.Properties[testWorkerRpcPortKey]).To(Equal(expectedPort))
				Expect(alluxioValue.Properties[testWorkerWebPortKey]).To(Equal(expectedPort))
				Expect(alluxioValue.Properties[testJobMasterRpcPortKey]).To(Equal(expectedPort))
				Expect(alluxioValue.Properties[testJobMasterWebPortKey]).To(Equal(expectedPort))
				Expect(alluxioValue.Properties[testJobWorkerRpcPortKey]).To(Equal(expectedPort))
				Expect(alluxioValue.Properties[testJobWorkerWebPortKey]).To(Equal(expectedPort))
				Expect(alluxioValue.Properties[testJobWorkerDataPortKey]).To(Equal(expectedPort))
			})
		})
	})
})
