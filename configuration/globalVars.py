import ConfigParser
import os


def init():
    get_config()

    global chameleon_tenant_id
    chameleon_tenant_id = ""

    global chameleonAuthURL
    try:
        chameleonAuthURL = config.get('GlobalInformation', 'chameleonAuthURL')
    except ConfigParser.NoOptionError:
        if (cloudSelect != "chameleon"):
	    pass
    
    global chameleonObjectStorageURL
    try:
        chameleonObjectStorageURL = config.get('GlobalInformation', 'chameleonObjectStorageURL')
    except ConfigParser.NoOptionError:
        if (cloudSelect != "chameleon"):
	    pass

    global chameleonTenantName
    try:
        chameleonTenantName = config.get('GlobalInformation', 'chameleonTenantName')
    except ConfigParser.NoOptionError:
        if (cloudSelect != "chameleon"):
	    pass

    global chameleonContainerName
    try:
        chameleonContainerName = config.get('GlobalInformation', 'chameleonContainerName')
    except ConfigParser.NoOptionError:
        if (cloudSelect != "chameleon"):
	    pass

    global chameleonCloudUsername
    try:
        chameleonCloudUsername = config.get('GlobalInformation', 'chameleonCloudUsername')
    except ConfigParser.NoOptionError:
        if (cloudSelect != "chameleon"):
	    pass

    global chameleonCloudPassword
    try:
        chameleonCloudPassword = config.get('GlobalInformation', 'chameleonCloudPassword')
    except ConfigParser.NoOptionError:
        if (cloudSelect != "chameleon"):
	    pass

    global uploadSize
    uploadSize = int(config.get('GlobalInformation', 'uploadSize'))
    uploadSize = uploadSize * 1024 * 1024

    global bufferSize
    bufferSize = int(config.get('GlobalInformation', 'bufferSize'))
    bufferSize = bufferSize * 1024 * 1024
    bufferSize = min(bufferSize, uploadSize)

    global cameraList
    cameraList = []
    sectionList = config.sections()
    for section in sectionList:
       if (section != 'GlobalInformation'):
          cameraList.append(dict(config.items(section)))

def get_config():
    global config
    config = ConfigParser.RawConfigParser()
    package_directory = os.path.dirname(os.path.abspath(__file__))
    config.read(os.path.join(package_directory, 'config.txt'))
