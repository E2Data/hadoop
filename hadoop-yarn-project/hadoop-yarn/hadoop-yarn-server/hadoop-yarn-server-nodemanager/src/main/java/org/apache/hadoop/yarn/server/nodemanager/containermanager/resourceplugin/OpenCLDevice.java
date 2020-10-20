package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;

import org.apache.hadoop.yarn.exceptions.YarnException;

// OpenCL-enabled GPU discovery utility
import com.nativelibs4java.opencl.*;
import static com.nativelibs4java.opencl.library.OpenCLLibrary.*;

/**
 * Merely a wrapper for the CLDevice object
 * in order to store platformId and deviceId
 * (information that is not held by the CLDevice class)
 */
public class OpenCLDevice {

  private int platformId;
  private int deviceId;
  private CLDevice device;

  public OpenCLDevice(CLDevice device, int platformId, int deviceId) {
    this.device = device;
    this.platformId = platformId;
    this.deviceId = deviceId;
  }

  public CLDevice getDevice()     { return this.device; }

  public int      getPlatformId() { return this.platformId; }

  public int      getDeviceId()   { return this.deviceId; }
}
