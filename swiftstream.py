from amcrest import AmcrestCamera
import chameleon.chameleonAuth as chameleonAuth
import configuration.globalVars as globalVars
import datetime
import libraries.BufferQueue as BufferQueue
import live555
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
def fill_queue(codecName, bytes, sec, usec, durUSec):
   # Check that we are saving only the video frames
   if not (codecName == 'H264'):
      return

   # Each frame must start with this byte sequence
   bytes = b'\0\0\0\1' + bytes
   
   # Use the global variables initialized by the director process
   global bytecount 
   global q

   amount = min(len(bytes), globalVars.uploadSize, (globalVars.uploadSize - bytecount))
   q.put(bytes[:amount])
   bytecount += amount

   if bytecount >= globalVars.uploadSize:
   
      #Close the buffer so that the consumer knows there is no more data
      q.close()

      #Put remainder of frame into the queue
      q.put(bytes[amount:])
      bytecount = len(bytes) - amount

# Initiates the camera feed and spawns the consumer / producer threads
def realtime_swift_stream(self, channel=1, typeno=0):

   stored_exception=None

   # Create a new buffer for streaming
   global q
   q = BufferQueue.BufferQueue(globalVars.bufferSize)

   # Counter to maintain upload size
   global bytecount
   bytecount = 0

   # Start the producer thread
   live555.startRTSP(cameraURL, fill_queue, False)
   producer = threading.Thread(target=live555.runEventLoop)
   producer.setDaemon(True)
   producer.start()

   # Loop until the break signal has been received
   try:
      while True:
         if stored_exception:
            break

         # Setup the filename for the uploaded segment
         ts = time.time()
         fileName = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
         fileName = cameraName + " " + fileName

         # Retrieve authentication for swift and setup storage location
         my_token_id = chameleonAuth.auth(tenantName)
         url = globalVars.chameleonObjectStorageURL + "/" + containerName + "/" + path + "/" + fileName
         my_headers = {"Content-Type": 'binary/octet-stream', "Transfer-Encoding": "chunked", "X-Auth-Token": my_token_id}
         
         consumer = threading.Thread(target=send_data, args=(q, url, my_headers,))

         consumer.start()

         #Wait for upload to complete, timeout in while loop allows keyboard interrupt to function
         while consumer.is_alive():
            consumer.join(timeout=1.0)

   except KeyboardInterrupt:
      stored_exception=sys.exc_info()
      quitEvent.set()
      live555.stopEventLoop()
      producer.join()
      q.close()
      consumer.join()
      print("Camera " + cameraName + " finished.")

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

         cameraURL = "rtsp://" + camera['username'] + ":" + camera['password'] + "@" + camera['hostname'] + ":554/cam/realmonitor?channel=1&subtype=0"

         camera = amcrest.camera
         cameraName = camera.machine_name[5:].strip()

         camera.realtime_swift_stream = types.MethodType( realtime_swift_stream, camera )
         processes.append(multiprocessing.Process(target=stream_video, name="streamvideo"+cameraName, args=()))
         processes[-1].start()

      for process in processes:
         process.join()

   except KeyboardInterrupt:
      print("The camera feeds will terminate after they have finished their current upload, please wait...")
      for process in processes:
         process.join()
