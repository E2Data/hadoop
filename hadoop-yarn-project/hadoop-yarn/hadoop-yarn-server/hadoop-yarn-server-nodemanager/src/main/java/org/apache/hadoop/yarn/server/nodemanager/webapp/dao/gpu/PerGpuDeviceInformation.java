/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Capture single GPU device information such as memory size, temperature,
 * utilization.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@XmlRootElement(name = "gpu")
public class PerGpuDeviceInformation {

  private String productName = "N/A";
  private Integer platformId = null;
  private Integer deviceId = null;
  private Long globalMemSize = null;

  public PerGpuDeviceInformation(String productName,
                                 int platformId,
                                 int deviceId,
                                 long globalMemSize) {
    setProductName(productName);
    setPlatformId(platformId);
    setDeviceId(deviceId);
    setGlobalMemSize(globalMemSize);
  }

  @XmlElement(name = "product_name")
  public String getProductName() {
    // product name is of the format:
    // <canonical_product_name>_<platform_id>_<device_id>
    return productName + "_" + platformId + "_" + deviceId;
  }

  public void setProductName(String productName) {
    this.productName = productName;
  }

  @XmlElement(name = "platform_id")
  public Integer getPlatformId() {
    return platformId;
  }

  public void setPlatformId(int platformId) {
    this.platformId = new Integer(platformId);
  }

  @XmlElement(name = "device_id")
  public Integer getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(int deviceId) {
    this.deviceId = new Integer(deviceId);
  }

  @XmlElement(name = "global_mem_size")
  public Long getGlobalMemSize() {
    return globalMemSize;
  }

  public void setGlobalMemSize(long globalMemSize) {
    this.globalMemSize = new Long(globalMemSize);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ProductName=").append(productName);

    if (getPlatformId() != null) {
      sb.append(", PlatformId=").append(getPlatformId());
    }

    if (getDeviceId() != null) {
      sb.append(", DeviceId=").append(getDeviceId());
    }

    if (getGlobalMemSize() != null) {
      sb.append(", GlobalMemorySize=").append(getGlobalMemSize() + " bytes");
    }

    return sb.toString();
  }
}
