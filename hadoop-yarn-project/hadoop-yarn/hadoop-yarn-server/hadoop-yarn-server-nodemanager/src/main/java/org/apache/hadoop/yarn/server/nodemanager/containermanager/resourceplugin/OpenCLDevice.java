package org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin;

import org.apache.hadoop.yarn.exceptions.YarnException;

// OpenCL-enabled GPU discovery utility
import static org.jocl.CL.*;
import org.jocl.*;

/**
 * Merely a wrapper for the CLDevice object
 * in order to store platformId and deviceId
 * (information that is not held by the CLDevice class)
 */
public class OpenCLDevice {

  private int platformId;
  private int deviceId;
  private cl_device_id device;

  public OpenCLDevice(cl_device_id device, int platformId, int deviceId) {
    this.device = device;
    this.platformId = platformId;
    this.deviceId = deviceId;
  }

  public String       getDeviceName() { return getString(this.device, CL_DEVICE_NAME); }

  public cl_device_id getDevice()     { return this.device; }

  public int          getPlatformId() { return this.platformId; }

  public int          getDeviceId()   { return this.deviceId; }

  /**
   * Returns the value of the device info parameter with the given name
   *
   * @param device The device
   * @param paramName The parameter name
   * @return The value
   */
  private static String getString(cl_device_id device, int paramName) {
    // Obtain the length of the string that will be queried
    long size[] = new long[1];
    clGetDeviceInfo(device, paramName, 0, null, size);

    // Create a buffer of the appropriate size and fill it with the info
    byte buffer[] = new byte[(int)size[0]];
    clGetDeviceInfo(device, paramName, buffer.length, Pointer.to(buffer), null);

    // Create a string from the buffer (excluding the trailing \0 byte)
    return new String(buffer, 0, buffer.length-1);
  }
}
