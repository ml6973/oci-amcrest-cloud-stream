from amcrest import AmcrestCamera
import chameleon.chameleonAuth as chameleonAuth
import configuration.globalVars as globalVars
import datetime
import libraries.BufferQueue as BufferQueue
import multiprocessing
import requests
import shutil
import sys
import time
import threading
import types

# Sends the data to Swift from the buffer (consumer)
def send_data(data, url, my_headers):
   r = requests.put(url, data=data, headers=my_headers)

# Takes the data from the camera and put it in the buffer (producer)
def fill_queue(data, queue):
   
   bytecount = 0
   
   #Loop until the uploadSize amount of bytes have been put into the buffer
   while bytecount < globalVars.uploadSize:
      #Stop filling the buffer if keyboard interrupt was detected
      if quitEvent.is_set():
         break
      amount = min(1000, globalVars.uploadSize, (globalVars.uploadSize - bytecount))
      queue.put(data.read(amount))
      bytecount += amount
   
   #Close the buffer so that the consumer knows there is no more data
   queue.close()

# Initiates the camera feed and spawns the consumer / producer threads
def realtime_swift_stream(self, channel=1, typeno=0):
   ret = self.command(
      'realmonitor.cgi?action=getStream&channel={0}&subtype={1}'.format(
         channel, typeno)
   )

   stored_exception=None

   # Loop until the break signal has been received
   try:
      while True:
         if stored_exception:
            break

         # Setup the filename for the uploaded segment
         ts = time.time()
         fileName = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
         fileName = camera.machine_name[5:].strip() + " " + fileName

         # Retrieve authentication for swift and setup storage location
         my_token_id = chameleonAuth.auth(tenantName)
         url = globalVars.chameleonObjectStorageURL + "/" + containerName + "/" + path + "/" + fileName
         my_headers = {"Content-Type": 'binary/octet-stream', "Transfer-Encoding": "chunked", "X-Auth-Token": my_token_id}

         # Create a new buffer for streaming
         q = BufferQueue.BufferQueue(globalVars.bufferSize)

         consumer = threading.Thread(target=send_data, args=(q, url, my_headers,))
         producer = threading.Thread(target=fill_queue, args=(ret.raw, q,))

         consumer.start()
         producer.start()

         #Wait for upload to complete, timeout in while loop allows keyboard interrupt to function
         while producer.is_alive() or consumer.is_alive():
            producer.join(timeout=1.0)
            consumer.join(timeout=1.0)

   except KeyboardInterrupt:
      stored_exception=sys.exc_info()
      quitEvent.set()
      print("Camera " + camera.machine_name[5:] + " finished.")

# Wrapper so that this function is spawned in another process
def stream_video():
   camera.realtime_swift_stream()

def stream_audio():
   camera.audio_stream_capture(httptype="singlepart", channel=1)

# Main function for initializing a process for each camera
if __name__ == '__main__':
   globalVars.init()
   quitEvent = threading.Event()
   processes = []

   try:
      for camera in globalVars.cameraList:

         amcrest = AmcrestCamera(camera['hostname'], camera['port'], camera['username'], camera['password'])

         tenantName = camera['chameleontenantname']
         containerName = camera['chameleoncontainername']
         path = camera['chameleonpath']
         path = path.lstrip("/")
         path = path.rstrip("/")

         camera = amcrest.camera

         camera.realtime_swift_stream = types.MethodType( realtime_swift_stream, camera )
         processes.append(multiprocessing.Process(target=stream_video, name="streamvideo"+camera.machine_name[5:], args=()))
         processes[-1].start()

      for process in processes:
         process.join()

   except KeyboardInterrupt:
      print("The camera feeds will terminate after they have finished their current upload, please wait...")
      for process in processes:
         process.join()
