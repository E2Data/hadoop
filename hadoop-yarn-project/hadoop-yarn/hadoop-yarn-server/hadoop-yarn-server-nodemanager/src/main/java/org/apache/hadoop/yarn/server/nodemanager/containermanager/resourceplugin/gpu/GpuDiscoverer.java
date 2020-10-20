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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.gpu;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.OpenCLDevice;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.GpuDeviceInformation;
import org.apache.hadoop.yarn.server.nodemanager.webapp.dao.gpu.PerGpuDeviceInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// OpenCL-enabled GPU discovery utility
import com.nativelibs4java.opencl.*;
import static com.nativelibs4java.opencl.library.OpenCLLibrary.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class GpuDiscoverer {
  public static final Logger LOG = LoggerFactory.getLogger(
      GpuDiscoverer.class);

  private static GpuDiscoverer instance;

  static {
    instance = new GpuDiscoverer();
  }

  private Configuration conf = null;

  private int numOfErrorExecutionSinceLastSucceed = 0;
  GpuDeviceInformation lastDiscoveredGpuInformation = null;

  private void validateConfOrThrowException() throws YarnException {
    if (conf == null) {
      throw new YarnException("Please initialize (call initialize) before use "
          + GpuDiscoverer.class.getSimpleName());
    }
  }

  /**
   * Get GPU device information from system.
   *
   * Please note that this only works on *NIX platform, so external caller
   * need to make sure this.
   *
   * @param productCanonicalName the name of the GPU(s) to look for
   * @return GpuDeviceInformation
   * @throws YarnException when any error happens
   */
  public synchronized GpuDeviceInformation getGpuDeviceInformation(String productCanonicalName)
      throws YarnException {
    validateConfOrThrowException();
    
    List<OpenCLDevice> OpenCLGpus =
        this.getOpenCLEnabledGpus(productCanonicalName);

    List<PerGpuDeviceInformation> gpus = new ArrayList<PerGpuDeviceInformation>();

    for (OpenCLDevice device : OpenCLGpus) {
      gpus.add(new PerGpuDeviceInformation(
        productCanonicalName,
        device.getPlatformId(),
        device.getDeviceId(),
        device.getDevice().getGlobalMemSize()
      ));
    }

    lastDiscoveredGpuInformation = new GpuDeviceInformation(gpus);
    return lastDiscoveredGpuInformation;

  }

  /**
   * Get list of GPU devices usable by YARN.
   *
   * @return List of GPU devices
   * @throws YarnException when any issue happens
   */
  public synchronized List<GpuDevice> getGpusUsableByYarn(String filterName)
      throws YarnException {
    validateConfOrThrowException();

    String allowedDevicesStr = conf.get(
        YarnConfiguration.NM_GPU_ALLOWED_DEVICES,
        YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES);

    List<GpuDevice> gpuDevices = new ArrayList<>();

    if (allowedDevicesStr.equals(
        YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES)) {
      // Get gpu device information from system.
      if (null == lastDiscoveredGpuInformation) {
        String msg = YarnConfiguration.NM_GPU_ALLOWED_DEVICES + " is set to "
            + YarnConfiguration.AUTOMATICALLY_DISCOVER_GPU_DEVICES
            + ", however automatically discovering "
            + "GPU information failed, please check NodeManager log for more"
            + " details, as an alternative, admin can specify "
            + YarnConfiguration.NM_GPU_ALLOWED_DEVICES
            + " manually to enable GPU isolation.";
        LOG.error(msg);
        throw new YarnException(msg);
      }

      if (lastDiscoveredGpuInformation.getGpus() != null) {
        for (int i = 0; i < lastDiscoveredGpuInformation.getGpus().size();
             i++) {
          List<PerGpuDeviceInformation> gpuInfos =
              lastDiscoveredGpuInformation.getGpus();
          String canonicalGpuProductNameWithPandD = canonicalProductName(gpuInfos.get(i).getProductName());
          String canonicalGpuProductName = canonicalGpuProductNameWithPandD.split("_")[0];
          // LOG.info("## SNIARCHOS ## : FilterName=" + filterName + ", CanonicalProductName=" + canonicalGpuProductName);
          if((filterName == null) || ((filterName != null) && (canonicalGpuProductName.equals(filterName)))) {
            gpuDevices.add(new GpuDevice(i, gpuInfos.get(i).getPlatformId(), gpuInfos.get(i).getDeviceId()));
          }
        }
      }
    } else{
      //TODO: Cannot handle this case yet. The allowed devices should be the AUTOMATICALLY_DISCOVER_GPU_DEVICES
      for (String s : allowedDevicesStr.split(",")) {
        if (s.trim().length() > 0) {
          String[] kv = s.trim().split(":");
          if (kv.length != 3) {
            throw new YarnException(
                "Illegal format, it should be index:platform_id:device_id format, now it="
                    + s);
          }

          gpuDevices.add(
              new GpuDevice(Integer.parseInt(kv[0]), Integer.parseInt(kv[1]), Integer.parseInt(kv[2])));
        }
      }
      LOG.info("Allowed GPU devices:" + gpuDevices);
    }

    return gpuDevices;
  }

  /**
   * Removes all spaces from a GPU product name and returns it in lower-case
   *
   * @param productName Product name of a GPU device as extracted by the nvidia-smi utility
   * @return Product name in normalized form
   */
  public static String canonicalProductName(String productName){

    return productName.replaceAll(" ", "").replaceAll("-","").toLowerCase();
  }

  public synchronized void initialize(Configuration conf, String productCanonicalName) {
    this.conf = conf;
    // Try to discover GPU information once and print
    try {
      LOG.info("Trying to discover GPU information ...");
      GpuDeviceInformation info = getGpuDeviceInformation(productCanonicalName);
      LOG.info(info.toString());
    } catch (YarnException e) {
      String msg =
          "Failed to discover GPU information from system, exception message:"
              + e.getMessage() + " continue...";
      LOG.warn(msg);
    }
  }

  /**
   * Retrieves the list of all OpenCL-enabled devices that correspond
   * to the provided (canonical) product name
   *
   * @param providedDeviceCanonicalName Canonical product name of interest
   * @param type The type of device (CLDevice.Type.{GPU,Accelerator})
   * @return list of CLDevice objects
   */
  private List<OpenCLDevice> getOpenCLEnabledGpus(String productCanonicalName)
  throws YarnException {

    List<OpenCLDevice> requestedDevices = new ArrayList<OpenCLDevice>();

    CLPlatform[] platforms = JavaCL.listPlatforms();

    for (int pid = 0; pid < platforms.length; pid++) {
      try {
        CLPlatform platform = platforms[pid];
        CLDevice[] devices = platform.listDevices(CLDevice.Type.GPU, false);
        for (int did = 0; did < devices.length; did++) {
          CLDevice device = devices[did];
          String deviceName = device.getName();
          String deviceCanonicalName = GpuDiscoverer.canonicalProductName(deviceName);
          if (deviceCanonicalName.equals(productCanonicalName)) {
            requestedDevices.add(new OpenCLDevice(device, pid, did));
          }
        }
      } catch (CLException e) {
        if (e.getCode() == CL_DEVICE_NOT_FOUND)
          continue;
        throw new YarnException("Unexpected OpenCL error", e);
      }
    }

    return requestedDevices;

  }

  public static GpuDiscoverer getInstance() {
    return instance;
  }
}
