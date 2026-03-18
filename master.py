import ctypes 
import time
import struct
from rdf import add_context
import threading
from dnp3protocol.dnp3api import *


# if SERVER_TCP_COMMUNICATION enabled tcp works else serial communication works 
SERVER_TCP_COMMUNICATION  = 1

# enbale to view traffic
# VIEW_TRAFFIC = 1

SERIAL_PORT_NUMBER  = 2

# print error code and description
def errorcodestring(errorcode):
    sDNP3ErrorCodeDes = sDNP3ErrorCode()
    sDNP3ErrorCodeDes.iErrorCode = errorcode
    dnp3_lib.DNP3ErrorCodeString(sDNP3ErrorCodeDes)
    return sDNP3ErrorCodeDes.LongDes.decode("utf-8")

# print error value and description
def errorvaluestring(errorvalue):
    sDNP3ErrorValueDes = sDNP3ErrorValue()
    sDNP3ErrorValueDes.iErrorValue = errorvalue   
    dnp3_lib.DNP3ErrorValueString(sDNP3ErrorValueDes)
    return sDNP3ErrorValueDes.LongDes.decode("utf-8")
  
# dnp3 debug callback
def cbDebug(u16ObjectId, ptDebugData, ptErrorValue):

    i16ErrorCode = ctypes.c_short()
    i16ErrorCode = 0 

    u16nav = ctypes.c_ushort()
    u16nav = 0

    #printf("cbDebug() called");

    #print(f"")    
    
    if (ptDebugData.contents.u32DebugOptions & eDebugOptionsFlag.DEBUG_OPTION_TX) == eDebugOptionsFlag.DEBUG_OPTION_TX : 

        if ptDebugData.contents.eCommMode   ==  eCommunicationMode.COMM_SERIAL:
        
            print(f"Serial port {ptDebugData.contents.u16ComportNumber} Transmit { ptDebugData.contents.u16TxCount} bytes ->  ", end='')
        
        else :
        
            print(f"IP {ptDebugData.contents.ai8IPAddress} Ethernet port {ptDebugData.contents.u16PortNumber} Transmit {ptDebugData.contents.u16TxCount} bytes ->  ", end='' )
        

        for u16nav in range(ptDebugData.contents.u16TxCount):
            print(f" {ptDebugData.contents.au8TxData[u16nav]:02x}", end='')
    

    if (ptDebugData.contents.u32DebugOptions & eDebugOptionsFlag.DEBUG_OPTION_RX) == eDebugOptionsFlag.DEBUG_OPTION_RX : 

        if ptDebugData.contents.eCommMode   ==  eCommunicationMode.COMM_SERIAL:
        
            print(f"Serial port {ptDebugData.contents.u16ComportNumber} Receive { ptDebugData.contents.u16RxCount} bytes <-  ", end='')
        
        else :
        
            print(f"IP {ptDebugData.contents.ai8IPAddress} Ethernet port {ptDebugData.contents.u16PortNumber} Receive {ptDebugData.contents.u16RxCount} bytes <-  ", end='' )
        
        

        for u16nav in range(ptDebugData.contents.u16RxCount):
            print(f" {ptDebugData.contents.au8RxData[u16nav]:02x}", end='')


    
    if (ptDebugData.contents.u32DebugOptions & eDebugOptionsFlag.DEBUG_OPTION_ERROR) == eDebugOptionsFlag.DEBUG_OPTION_ERROR: 
    
        print(f"Error message {ptDebugData.contents.au8ErrorMessage}")
        print(f"ErrorCode  {ptDebugData.contents.i16ErrorCode}")
        print(f"ErrorValue {ptDebugData.contents.tErrorValue}")
    

    if (ptDebugData.contents.u32DebugOptions & eDebugOptionsFlag.DEBUG_OPTION_WARNING) == eDebugOptionsFlag.DEBUG_OPTION_WARNING: 
    
        print(f"Error message {ptDebugData.contents.au8WarningMessage}")
        print(f"ErrorCode  {ptDebugData.contents.i16ErrorCode}")
        print(f"ErrorValue {ptDebugData.contents.tErrorValue}")

    print("", flush=True)

    return i16ErrorCode

# dnp3 client connection status callback
def cbClientStatus(u16ObjectId,psDAID,peSat,ptErrorValue):

    i16ErrorCode = ctypes.c_short()
    i16ErrorCode = 0   

    print("************cbClientStatus called*****************")
    print(" Client ID : %u" % u16ObjectId)


    if psDAID.contents.eCommMode    ==  eCommunicationMode.COMM_SERIAL:    
        print("Serial Port %u"% psDAID.contents.u16SerialPortNumber)    
    else :    
        print("IP Address %s" % psDAID.contents.ai8IPAddress)
        print("\tPort %u" % psDAID.contents.u16PortNumber)


    print("\t Server Address :%u" % psDAID.contents.u16SlaveAddress)

    if peSat.contents.value == eServerConnectionStatus.SERVER_CONNECTED:    
        print("Server Connected")    
    else :
        print("Server Not connected")
    
    return i16ErrorCode

#dnp3 device attrribute callback
def cbDeviceAtt(u16ObjectId,psDAID,psDeviceAttrValue,ptErrorValue):

    i16ErrorCode = ctypes.c_short()
    i16ErrorCode = 0   

    print("***********cbDeviceAtt called*****************")
    print(" Client ID : %u" % u16ObjectId)


    if psDAID.contents.eCommMode    ==  eCommunicationMode.COMM_SERIAL:    
        print("Serial Port %u"% psDAID.contents.u16SerialPortNumber)    
    else :    
        print("IP Address %s" % psDAID.contents.ai8IPAddress)
        print("\tPort %u" % psDAID.contents.u16PortNumber)


    print("\t Server Address :%u" % psDAID.contents.u16SlaveAddress)

    print("Variation %u" % psDeviceAttrValue.contents.u8Variation)
    print("Datatype %u" % psDeviceAttrValue.contents.u8Datatype)
    print("Length %u" % psDeviceAttrValue.contents.u16Length)

    '''datatype
    //1- vstr
    //2-uint
    //3-int
    //4-float
    //5-ostr
    //6-bstr
    //254 -u8bs8list
    '''

    while True:
        if psDeviceAttrValue.contents.u16Length == 0:
            break

        if psDeviceAttrValue.contents.u8Datatype == 1:
            print(f" VSTR : {psDeviceAttrValue.contents.u8Data}")
            break

        if psDeviceAttrValue.contents.u8Datatype == 254:
            nu8Count = 0
            while nu8Count < psDeviceAttrValue.contents.u16Length:
                print(f" \r\n Variation number {psDeviceAttrValue.contents.u8Data[nu8Count]}")
                nu8Count += 1
                print(f" Attribute Properties {psDeviceAttrValue.contents.u8Data[nu8Count]}")
                nu8Count += 1
            break

        
        for nu8Count in range(psDeviceAttrValue.contents.u16Length):
            print(f"  {psDeviceAttrValue.contents.u8Data[nu8Count]:02x}")

        break




    return i16ErrorCode

#dnp3 class poll status callback 
def cbPollStatus(u16ObjectId,ptUpdateID,eFunctionID,ptErrorValue):

    i16ErrorCode = ctypes.c_short()
    i16ErrorCode = 0   

    print("***********cbPollStatus called*****************")
    print(" Client ID : %u" % u16ObjectId)


    if ptUpdateID.contents.eCommMode    ==  eCommunicationMode.COMM_SERIAL:    
        print("Serial Port %u"% ptUpdateID.contents.u16SerialPortNumber)    
    else :    
        print("IP Address %s" % ptUpdateID.contents.ai8IPAddress)
        print("\tPort %u" % ptUpdateID.contents.u16PortNumber)


    print("\t Server Address :%u" % ptUpdateID.contents.u16SlaveAddress)

    if eFunctionID == eWriteFunctionID.READCLASS0123:
        print(" integrity poll class 0123 completed")
    else:
        print(" Invalid function id")

    return i16ErrorCode

#dnp3 update IIN BITS CALLBACK
def cbUpdateIIN(u16ObjectId,ptUpdateID,u8IIN1,u8IIN2,ptErrorValue):

    i16ErrorCode = ctypes.c_short()
    i16ErrorCode = 0   

    print("***********cbUpdateIIN called*****************")
    print(" Client ID : %u" % u16ObjectId)


    if ptUpdateID.contents.eCommMode    ==  eCommunicationMode.COMM_SERIAL:    
        print("Serial Port %u"% ptUpdateID.contents.u16SerialPortNumber)    
    else :    
        print("IP Address %s" % ptUpdateID.contents.ai8IPAddress)
        print("\tPort %u" % ptUpdateID.contents.u16PortNumber)


    print("\t Server Address :%u" % ptUpdateID.contents.u16SlaveAddress)

    if(u8IIN1 & eIINFirstByteBitsFlag.BROADCAST) == eIINFirstByteBitsFlag.BROADCAST:
        print("\t BROADCAST")  

    if(u8IIN1 & eIINFirstByteBitsFlag.CLASS_1_EVENTS) == eIINFirstByteBitsFlag.CLASS_1_EVENTS:
        print("\t CLASS_1_EVENTS")

    if(u8IIN1 & eIINFirstByteBitsFlag.CLASS_2_EVENTS) == eIINFirstByteBitsFlag.CLASS_2_EVENTS:
        print("\t CLASS_2_EVENTS")    

    if(u8IIN1 & eIINFirstByteBitsFlag.CLASS_3_EVENTS) == eIINFirstByteBitsFlag.CLASS_3_EVENTS:
        print("\t CLASS_3_EVENTS")    

    if(u8IIN1 & eIINFirstByteBitsFlag.NEED_TIME) == eIINFirstByteBitsFlag.NEED_TIME:
        print("\t NEED_TIME")    

    if(u8IIN1 & eIINFirstByteBitsFlag.LOCAL_CONTROL) == eIINFirstByteBitsFlag.LOCAL_CONTROL:
        print("\t LOCAL_CONTROL")   

    if(u8IIN1 &  eIINFirstByteBitsFlag.DEVICE_TROUBLE) ==  eIINFirstByteBitsFlag.DEVICE_TROUBLE:
        print("\t  DEVICE_TROUBLE")    

    if(u8IIN1 &  eIINFirstByteBitsFlag.DEVICE_RESTART ) ==  eIINFirstByteBitsFlag.DEVICE_RESTART :    
        print("\t  DEVICE_RESTART")    

    if(u8IIN2 &  eIINSecondByteBitsFlag.NO_FUNC_CODE_SUPPORT ) ==   eIINSecondByteBitsFlag.NO_FUNC_CODE_SUPPORT :    
        print("\t  NO_FUNC_CODE_SUPPORT")
    
    if(u8IIN2 &   eIINSecondByteBitsFlag.OBJECT_UNKNOWN ) ==    eIINSecondByteBitsFlag.OBJECT_UNKNOWN:    
        print("\t  OBJECT_UNKNOWN ")
    
    if(u8IIN2 &   eIINSecondByteBitsFlag.PARAMETER_ERROR ) ==   eIINSecondByteBitsFlag.PARAMETER_ERROR:    
        print("\t PARAMETER_ERROR ")    

    if(u8IIN2 &    eIINSecondByteBitsFlag.EVENT_BUFFER_OVERFLOW) ==    eIINSecondByteBitsFlag.EVENT_BUFFER_OVERFLOW:    
        print("\t  EVENT_BUFFER_OVERFLOW")    

    if(u8IIN2 &    eIINSecondByteBitsFlag.ALREADY_EXECUTING) ==   eIINSecondByteBitsFlag.ALREADY_EXECUTING:    
        print("\t ALREADY_EXECUTING ")    

    if(u8IIN2 &   eIINSecondByteBitsFlag.CONFIG_CORRUPT ) ==   eIINSecondByteBitsFlag.CONFIG_CORRUPT:    
        print("\t CONFIG_CORRUPT ")    

    if(u8IIN2 &   eIINSecondByteBitsFlag.RESERVED_2 ) ==   eIINSecondByteBitsFlag.RESERVED_2:    
        print("\t RESERVED_2 ")    

    if(u8IIN2 &   eIINSecondByteBitsFlag.RESERVED_1 ) ==   eIINSecondByteBitsFlag.RESERVED_1:    
        print("\t  RESERVED_1")

    return i16ErrorCode

# SEND command for paticular group and index from user input
def sendCommand(myClient):

    i16ErrorCode = ctypes.c_short()
    tErrorValue =  ctypes.c_short()
    
    print("sendCommand Called")
    while True:
        try:
            u16index = ctypes.c_uint16(int(input("Analog output command Enter Index address ")))
        except ValueError:
            print("Please enter a number ")
        else:
            break

    while True:
        try:
            f32value = ctypes.c_float(float(input("Enter command float value: ")))
        except ValueError:
            print("Please enter a float number ")
        else:
            break

   
    psDAID = sDNP3DataAttributeID()
    psNewValue  = sDNP3DataAttributeData()
    psCommandParameters = sDNP3CommandParameters()

    if  'SERVER_TCP_COMMUNICATION' in globals():
        psDAID.eCommMode    =   eCommunicationMode.TCP_IP_MODE
        psDAID.u16PortNumber    =   20000
        psDAID.ai8IPAddress =  "10.1.114.34".encode('utf-8')
    else:
        psDAID.eCommMode    =   eCommunicationMode.COMM_SERIAL
        psDAID.u16SerialPortNumber  =   2

    psDAID.eGroupID =   eDNP3GroupID.BINARY_OUTPUT
    psDAID.u16SlaveAddress  =   1024
    psDAID.u16IndexNumber   =   u16index
    psNewValue.eDataSize   =   eDataSizes.FLOAT32_SIZE
    psNewValue.eDataType   =   eDataTypes.FLOAT32_DATA
    psNewValue.tQuality        =   eDNP3QualityFlags.GOOD
    psNewValue.pvData          =   ctypes.cast(ctypes.pointer(f32value),ctypes.c_void_p)
    psCommandParameters.u8Count    =   1
    psCommandParameters.eCommandVariation = eCommandObjectVariation.ANALOG_OUTPUT_BLOCK_FLOAT32

    now = time.time()
    timeinfo = time.localtime(now)
    
    #current date
    psNewValue.sTimeStamp.u8Day = timeinfo.tm_mday
    psNewValue.sTimeStamp.u8Month = timeinfo.tm_mon
    psNewValue.sTimeStamp.u16Year = timeinfo.tm_year 

    psNewValue.sTimeStamp.u8Hour = timeinfo.tm_hour
    psNewValue.sTimeStamp.u8Minute = timeinfo.tm_min
    psNewValue.sTimeStamp.u8Seconds = timeinfo.tm_sec
    psNewValue.sTimeStamp.u16MilliSeconds = 0
    psNewValue.sTimeStamp.u16MicroSeconds = 0
    psNewValue.sTimeStamp.i8DSTTime = 0
    psNewValue.sTimeStamp.u8DayoftheWeek = 4
    psNewValue.bTimeInvalid = False

    #printf(" update float value %f",f32data);
    # Update server
   
    i16ErrorCode = dnp3_lib.DNP3DirectOperate(myClient, ctypes.byref(psDAID), ctypes.byref(psNewValue),ctypes.byref(psCommandParameters),ctypes.byref((tErrorValue)))
    if i16ErrorCode != 0:
        message = f"DNP3 Library API Function - DNP3DirectOperate() failed: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
        print(message)     

    else:
        message = f"DNP3 Library API Function - DNP3DirectOperate() success: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
        print(message) 

#Client Class
class DNP3_Master:
    def __init__(self, solid_server=None, ip_addr=0, port=20000, slave_addr=1024):
        self._callback_wrappers = []
        self.solid_server = solid_server
        self.myClient = None

        self.ip_addr = ip_addr
        self.port = port
        self.slave_addr = slave_addr
        self.buffer = {}
        self.buffer_lock = threading.Lock()
        threading.Thread(target=self.upload_buffer, daemon=True).start()

    def _wrap(self, callback_type, func):
        """Helper to wrap and persist callbacks."""
        if func is None:
            return ctypes.cast(None, callback_type)
        wrapper = callback_type(func)
        self._callback_wrappers.append(wrapper)
        return wrapper
    
    def start(self):
        print(" \t\t**** DNP3 Protocol Client Library Test ****")
        # Check library version against the library header file
        if dnp3_lib.DNP3GetLibraryVersion().decode("utf-8") != DNP3_VERSION:
            print(" Error: Version Number Mismatch")
            print(" Library Version is  : {}".format(dnp3_lib.DNP3GetLibraryVersion().decode("utf-8")))
            print(" The Header Version used is : {}".format(DNP3_VERSION))
            print("")
            input(" Press Enter to free dnp3 cLIENT object")
            exit(0)

        print(" Library Version is : {}".format(dnp3_lib.DNP3GetLibraryVersion().decode("utf-8")))
        print(" Library Build on   : {}".format(dnp3_lib.DNP3GetLibraryBuildTime().decode("utf-8")))
        print(" Library License Information   : {}".format(dnp3_lib.DNP3GetLibraryLicenseInfo().decode("utf-8")))

        i16ErrorCode = ctypes.c_short()
        tErrorValue =  ctypes.c_short() 
        sParameters = sDNP3Parameters()

        sParameters.eAppFlag          =  eApplicationFlag.APP_CLIENT
        sParameters.ptReadCallback = self._wrap(DNP3ReadCallback, None)
        sParameters.ptWriteCallback = self._wrap(DNP3WriteCallback, None)
        sParameters.ptUpdateCallback = self._wrap(DNP3UpdateCallback, self.cbUpdate)
        sParameters.ptSelectCallback = self._wrap(DNP3ControlSelectCallback, None)
        sParameters.ptOperateCallback = self._wrap(DNP3ControlOperateCallback, None)
        sParameters.ptDebugCallback = self._wrap(DNP3DebugMessageCallback, cbDebug)
        sParameters.ptUpdateIINCallback = self._wrap(DNP3UpdateIINCallback, cbUpdateIIN)
        sParameters.ptClientPollStatusCallback = self._wrap(DNP3ClientPollStatusCallback, cbPollStatus)
        sParameters.ptClientStatusCallback = self._wrap(DNP3ClientStatusCallback, cbClientStatus)
        sParameters.ptColdRestartCallback = self._wrap(DNP3ColdRestartCallback, None)
        sParameters.ptWarmRestartCallback = self._wrap(DNP3WarmRestartCallback, None)
        sParameters.ptDeviceAttrCallback = self._wrap(DNP3DeviceAttributeCallback, cbDeviceAtt)

        sParameters.u32Options        = 0
        sParameters.u16ObjectId				= 1				#Server ID which used in callbacks to identify the iec 104 server object   

        
        
        # Create a client object

        self.myClient =  dnp3_lib.DNP3Create(ctypes.byref(sParameters), ctypes.byref((i16ErrorCode)), ctypes.byref((tErrorValue)))
        if i16ErrorCode.value != 0:
            message = f"DNP3 Library API Function - DNP3Create() failed: {i16ErrorCode.value} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message)    
            exit(0) 
        else:
            message = f"DNP3 Library API Function -DNP3Create() success: {i16ErrorCode.value} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message) 

        sDNP3Config = sDNP3ConfigurationParameters()


        # Debug option settings
        if  'VIEW_TRAFFIC' in globals():
            sDNP3Config.sDNP3ClientSet.sDebug.u32DebugOptions   =   (eDebugOptionsFlag.DEBUG_OPTION_RX | eDebugOptionsFlag.DEBUG_OPTION_TX)
        else:
            sDNP3Config.sDNP3ClientSet.sDebug.u32DebugOptions  =   0


        now = time.time()
        timeinfo = time.localtime(now)
        
        #current date
        sDNP3Config.sDNP3ClientSet.sTimeStamp.u8Day = timeinfo.tm_mday
        sDNP3Config.sDNP3ClientSet.sTimeStamp.u8Month = timeinfo.tm_mon
        sDNP3Config.sDNP3ClientSet.sTimeStamp.u16Year = timeinfo.tm_year 

        sDNP3Config.sDNP3ClientSet.sTimeStamp.u8Hour = timeinfo.tm_hour
        sDNP3Config.sDNP3ClientSet.sTimeStamp.u8Minute = timeinfo.tm_min
        sDNP3Config.sDNP3ClientSet.sTimeStamp.u8Seconds = timeinfo.tm_sec
        sDNP3Config.sDNP3ClientSet.sTimeStamp.u16MilliSeconds = 0
        sDNP3Config.sDNP3ClientSet.sTimeStamp.u16MicroSeconds = 0
        sDNP3Config.sDNP3ClientSet.sTimeStamp.i8DSTTime = 0
        sDNP3Config.sDNP3ClientSet.sTimeStamp.u8DayoftheWeek = 4
        sDNP3Config.sDNP3ClientSet.bTimeInvalid = False


        sDNP3Config.sDNP3ClientSet.benabaleUTCtime = False # enable utc time/ local time
        sDNP3Config.sDNP3ClientSet.bUpdateCallbackCheckTimestamp = False # if it true ,the timestamp change also create the updatecallback 


        sDNP3Config.sDNP3ClientSet.u16NoofClient        =   1

        arraypointer = (sClientObject * sDNP3Config.sDNP3ClientSet.u16NoofClient )()
        sDNP3Config.sDNP3ClientSet.psClientObjects  = ctypes.cast(arraypointer, ctypes.POINTER(sClientObject))
        if  'SERVER_TCP_COMMUNICATION' in globals():
            arraypointer[0].eCommMode                     =   eCommunicationMode.TCP_IP_MODE
            # check computer configuration - TCP/IP Address
            arraypointer[0].sClientCommunicationSet.sEthernetCommsSet.ai8ToIPAddress = self.ip_addr.encode('utf-8')  # Server works on every interface
            arraypointer[0].sClientCommunicationSet.sEthernetCommsSet.u16PortNumber   =   self.port

        #Server protocol settings
        arraypointer[0].sClientProtSet.u16MasterAddress			=   1
        arraypointer[0].sClientProtSet.u16SlaveAddress            =   self.slave_addr
        arraypointer[0].sClientProtSet.u32LinkLayerTimeout        =   10000
        arraypointer[0].sClientProtSet.u32ApplicationTimeout      =   20000
        arraypointer[0].sClientProtSet.u32Class0123pollInterval   =   60000
        arraypointer[0].sClientProtSet.u32Class123pollInterval    =   1000
        arraypointer[0].sClientProtSet.u32Class0pollInterval      =   1000                              #CLASS 0 poll interval in milliSeconds (minimum 1000ms - to max)
        arraypointer[0].sClientProtSet.u32Class1pollInterval      =   0                              #CLASS 1 poll interval in milliSeconds (minimum 1000ms - to max)
        arraypointer[0].sClientProtSet.u32Class2pollInterval      =   0                              #CLASS 2 poll interval in milliSeconds (minimum 1000ms - to max)
        arraypointer[0].sClientProtSet.u32Class3pollInterval      =   0                              #CLASS 3 poll interval in milliSeconds (minimum 1000ms - to max)
        arraypointer[0].sClientProtSet.bFrozenAnalogInputSupport  =   False                          #False- stack will not create points for frozen analog input.
        arraypointer[0].sClientProtSet.bEnableFileTransferSupport =   False
        arraypointer[0].sClientProtSet.bDisableUnsolicitedStatup  =   False
        arraypointer[0].u32CommandTimeout                         =   50000
        arraypointer[0].u32FileOperationTimeout                   =   200000
        arraypointer[0].sClientProtSet.bDisableResetofRemotelink  =   False                      	# if it true ,client will not send the reset of remote link in startup
        arraypointer[0].sClientProtSet.eLinkConform = eLinkLayerConform.CONFORM_NEVER										# Data link layer confirmation default - CONFORM_NEVER
                

        sDNP3Config.sDNP3ClientSet.bAutoGenDNP3DataObjects  = True
        #Define number of objects
        arraypointer[0].u16NoofObject                              =   0
        #Allocate memory for objects0
        arraypointer[0].psDNP3Objects = None

        i16ErrorCode =  dnp3_lib.DNP3LoadConfiguration(self.myClient, ctypes.byref(sDNP3Config), ctypes.byref((tErrorValue)))
        if i16ErrorCode != 0:
            message = f"DNP3 Library API Function - DNP3IEC104LoadConfiguration() failed: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message)
            raise RuntimeError(message)

            

        else:
            message = f"DNP3 Library API Function - DNP3IEC104LoadConfiguration() success: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message) 



        i16ErrorCode =  dnp3_lib.DNP3Start(self.myClient, ctypes.byref((tErrorValue)))
        if i16ErrorCode != 0:
            message = f"DNP3 Library API Function - DNP3Start() failed: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message)
            raise RuntimeError(message)    
            

        else:
            message = f"DNP3 Library API Function - DNP3Start() success: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message)




        # print("press x to exit")

        # while(True):
        #     if keyboard.is_pressed('x'):
        #         print("x pressed, exiting loop")
        #         keyboard.release('x')
        #         time.sleep(0.1)
        #         break
        #     elif keyboard.is_pressed('s'):
        #         print("u pressed, send command called")
        #         keyboard.release('s')
        #         time.sleep(0.1)
        #         sendCommand(myClient)

        #     #Xprint("sleep called")
        #     time.sleep(0.1)

    def stopMaster(self):     
        tErrorValue =  ctypes.c_short()

        i16ErrorCode =  dnp3_lib.DNP3Stop(self.myClient, ctypes.byref((tErrorValue)))
        if i16ErrorCode != 0:
            message = f"DNP3 Library API Function - DNP3Stop() failed: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message)
        else:
            message = f"DNP3 Library API Function - DNP3Stop() success: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message) 



        i16ErrorCode =  dnp3_lib.DNP3Free(self.myClient, ctypes.byref((tErrorValue)))
        if i16ErrorCode != 0:
            message = f"DNP3 Library API Function - DNP3Free() failed: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message)    
        else:
            message = f"DNP3 Library API Function - DNP3Free() success: {i16ErrorCode} - {errorcodestring(i16ErrorCode)}, {tErrorValue.value} - {errorvaluestring(tErrorValue)}"
            print(message) 

    
        print("Exiting the program...")

    def cbUpdate(self, u16ObjectId, ptUpdateID, ptUpdateValue,ptUpdateParams,ptErrorValue):

        i16ErrorCode = ctypes.c_short()
        i16ErrorCode = 0   

        print("************cbUpdate called*****************")
        print(" Client ID : %u" % u16ObjectId)


        if ptUpdateID.contents.eCommMode    ==  eCommunicationMode.COMM_SERIAL:    
            print("Serial Port %u"% ptUpdateID.contents.u16SerialPortNumber)    
        else :    
            print("IP Address %s" % ptUpdateID.contents.ai8IPAddress)
            print("\tPort %u" % ptUpdateID.contents.u16PortNumber)

        print("\t Group ID :%u" % ptUpdateID.contents.eGroupID)
        print("\t Server Address :%u" % ptUpdateID.contents.u16SlaveAddress)
        print("\t Index No :%u" % ptUpdateID.contents.u16IndexNumber)
        print(f" Datatype->{ptUpdateValue.contents.eDataType} Datasize->{ ptUpdateValue.contents.eDataSize}" )

        #setup turtle info for solid
        slave_id = ptUpdateID.contents.u16SlaveAddress
        index = ptUpdateID.contents.u16IndexNumber
        group = ptUpdateID.contents.eGroupID
        #timestamp
        ts = ptUpdateValue.contents.sTimeStamp
        timestamp_str = f"{ts.u16Year:04}-{ts.u8Month:02}-{ts.u8Day:02}T{ts.u8Hour:02}:{ts.u8Minute:02}:{ts.u8Seconds:02}"

        current_value = None
        data_type_label = ""
    
        GroupID = ptUpdateID.contents.eGroupID
        
        if  GroupID in (eDNP3GroupID.BINARY_INPUT, eDNP3GroupID.DOUBLE_INPUT, eDNP3GroupID.BINARY_OUTPUT):
            data = bytearray(ctypes.string_at(ptUpdateValue.contents.pvData, 1))
            u8data = struct.unpack('B', data)[0] 
            print(f" Data : {u8data}")

            current_value = u8data
            data_type_label = "Binary"

        elif  GroupID in (eDNP3GroupID.COUNTER_INPUT, eDNP3GroupID.FRCOUNTER_INPUT):
            data = bytearray(ctypes.string_at(ptUpdateValue.contents.pvData, 4))
            i32data = struct.unpack('i', data)[0]        
            print(f" Data : {i32data}")

            data_type_label = "Counter" if group == eDNP3GroupID.COUNTER else "FrozenCounter"
            current_value = i32data

        elif  GroupID in (eDNP3GroupID.ANALOG_INPUT, eDNP3GroupID.FRANALOG_INPUT, eDNP3GroupID.ANALOG_OUTPUTS):
            if ptUpdateValue.contents.eDataType == eDataTypes.FLOAT32_DATA:
                data = bytearray(ctypes.string_at(ptUpdateValue.contents.pvData, 4))
                f32data = struct.unpack('f', data)[0] 
                print(f" Data : {f32data:.3f}")

                data_type_label = "Int32" if group == eDNP3GroupID.FRANALOG_INPUT else "FrozenAnalog"
                current_value = f32data

            elif ptUpdateValue.contents.eDataType == eDataTypes.SIGNED_DWORD_DATA:
                data = bytearray(ctypes.string_at(ptUpdateValue.contents.pvData, 4))
                i32data = struct.unpack('i', data)[0]        
                print(f" Data : {i32data}")

                data_type_label = "Int32"
                current_value = i32data
            else :
                print(" Invalid datatype in update -  analog")

        elif  GroupID in (eDNP3GroupID.OCTECT_STRING, eDNP3GroupID.VIRTUAL_TERMINAL_OUTPUT):
            data = bytearray(ctypes.string_at(ptUpdateValue.contents.pvData, ptUpdateValue.contents.eDataSize))
            #sdata = struct.unpack('p', data)[0]        
            print(f" Data : {data.decode("utf-8")}")
        else:
            print("\t Invalid Group ID")
        
        # --- Trigger Solid Update if we have valid data ---
        if current_value is not None and self.solid_server:
            self.fill_buffer(slave_id=slave_id, group=group,index=index,value=current_value,data_type=data_type_label,timestamp_str=timestamp_str)
        

        if ptUpdateValue.contents.sTimeStamp.u16Year != 0:
            print(f" Date : {ptUpdateValue.contents.sTimeStamp.u8Day:02}-{ptUpdateValue.contents.sTimeStamp.u8Month:02}-{ptUpdateValue.contents.sTimeStamp.u16Year:04}  DOW -{ptUpdateValue.contents.sTimeStamp.u8DayoftheWeek}")
            print(f" Time : {ptUpdateValue.contents.sTimeStamp.u8Hour:02}:{ptUpdateValue.contents.sTimeStamp.u8Minute:02}:{ptUpdateValue.contents.sTimeStamp.u8Seconds:02}:{ptUpdateValue.contents.sTimeStamp.u16MilliSeconds:03}")

        if(ptUpdateValue.contents.tQuality & eDNP3QualityFlags.ONLINE) == eDNP3QualityFlags.ONLINE:
            print(" ONLINE")
        
        if(ptUpdateValue.contents.tQuality & eDNP3QualityFlags.RESTART) == eDNP3QualityFlags.RESTART:
            print(" RESTART")
        
        if(ptUpdateValue.contents.tQuality & eDNP3QualityFlags.COMM_LOST) == eDNP3QualityFlags.COMM_LOST:
            print(" COMM_LOST")
        
        if(ptUpdateValue.contents.tQuality & eDNP3QualityFlags.REMOTE_FORCED) == eDNP3QualityFlags.REMOTE_FORCED:
            print(" REMOTE_FORCED")
        
        if(ptUpdateValue.contents.tQuality & eDNP3QualityFlags.LOCAL_FORCED) == eDNP3QualityFlags.LOCAL_FORCED:
            print(" LOCAL_FORCED")
        
        if(ptUpdateValue.contents.tQuality & eDNP3QualityFlags.CHATTER_FILTER) == eDNP3QualityFlags.CHATTER_FILTER:
            print(" CHATTER_FILTER")
        
        if(ptUpdateValue.contents.tQuality & eDNP3QualityFlags.ROLLOVER) == eDNP3QualityFlags.ROLLOVER:
            print(" ROLLOVER")
        
        if(ptUpdateValue.contents.tQuality & eDNP3QualityFlags.OVER_RANGE) == eDNP3QualityFlags.OVER_RANGE:
            print(" OVER_RANGE")
        
        if(ptUpdateValue.contents.tQuality & eDNP3QualityFlags.DISCONTINUITY) == eDNP3QualityFlags.DISCONTINUITY:
            print(" DISCONTINUITY")    

        if(ptUpdateValue.contents.tQuality & eDNP3QualityFlags.REFERENCE_ERR) == eDNP3QualityFlags.REFERENCE_ERR:
            print(" REFERENCE_ERR")

        return i16ErrorCode
    
    def fill_buffer(self, slave_id, group,index, value, data_type, timestamp_str):
        if not slave_id in self.buffer:
            self.buffer[slave_id] = {}

        if not group in self.buffer[slave_id]:
            self.buffer[slave_id][group] = {}

        if not index in self.buffer[slave_id][group]:
            self.buffer[slave_id][group][index] = {"values":[],"timestamps":[],"data_type":data_type}

        with self.buffer_lock:
            self.buffer[slave_id][group][index]["values"].append(value)
            self.buffer[slave_id][group][index]["timestamps"].append(timestamp_str)


        
        pass

    def upload_buffer(self):
        time.sleep(60)
        for slave_id, slave_dict in self.buffer.items():
            for group, group_dict in slave_dict.items():
                for index, index_dict in group_dict.items():
                    with self.buffer_lock:
                        index_copy = index_dict.copy()
                        index_dict["values"] = []
                        index_dict["timestamps"] = []
                    if len(index_copy["values"]) == 0:
                        print("NOT ENOUGH VALUES \n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
                        continue
                    rdf_data = add_context(
                        local_address=slave_id,
                        group=group,
                        index=index,
                        value=index_copy["values"],
                        data_type=index_copy["data_type"],
                        timestamp=index_copy["timestamps"]
                    )

                    threading.Thread(target=self.solid_server.append, args=(rdf_data,), daemon=True).start()

        self.upload_buffer()

    
    def sendCommand(self, index, signal):
        tErrorValue = ctypes.c_short(0)
        
        print("\n--- Typhoon HIL Command (Double-Point Sync) ---")
        index = index
        choice = signal
        
        # 1. DNP3 Double-bit values: 1 = OFF, 2 = ON
        db_value = 2 if choice == 1 else 1
        u8Data = ctypes.c_uint8(db_value)

        psDAID = sDNP3DataAttributeID()
        psValue = sDNP3DataAttributeData()
        psParams = sDNP3CommandParameters()

        # 2. Setup ID
        psDAID.eCommMode = eCommunicationMode.TCP_IP_MODE
        psDAID.ai8IPAddress = "10.1.114.34".encode('utf-8')
        psDAID.u16PortNumber = 20000
        psDAID.u16SlaveAddress = 1024 
        psDAID.u16IndexNumber = index
        psDAID.eGroupID = 10 # BINARY_OUTPUT

        # 3. THE FIX: Match Type 2 (DOUBLE_POINT_DATA)
        psValue.eDataType = 2  # This bypassed -1526
        psValue.eDataSize = 1  # DOUBLE_POINT_SIZE (typically 1 byte in this lib)
        psValue.tQuality = eDNP3QualityFlags.GOOD
        psValue.pvData = ctypes.cast(ctypes.pointer(u8Data), ctypes.c_void_p)

        # 4. Command Parameters (The Protocol Action)
        psParams.eCommandVariation = 1 # CROB_G12V1
        psParams.eOPType = 3 if choice == 1 else 4 # Latch On / Latch Off
        psParams.u8Count = 1
        psParams.u32ONtime = 100
        psParams.u32OFFtime = 100

        # Timestamp (Required for Operate)
        now = time.time()
        ti = time.localtime(now)
        psValue.sTimeStamp.u8Day, psValue.sTimeStamp.u8Month = ti.tm_mday, ti.tm_mon
        psValue.sTimeStamp.u16Year = ti.tm_year
        psValue.sTimeStamp.u8Hour, psValue.sTimeStamp.u8Minute = ti.tm_hour, ti.tm_min
        psValue.sTimeStamp.u8Seconds = ti.tm_sec

        # 5. EXECUTE
        print(f"Sending Command to Index {index} using Double-Point Logic...")
        i16ErrorCode = dnp3_lib.DNP3DirectOperate(
            self.myClient, 
            ctypes.byref(psDAID), 
            ctypes.byref(psValue),
            ctypes.byref(psParams),
            ctypes.byref(tErrorValue)
        )

        if i16ErrorCode == 0:
            print("SUCCESS: Command accepted and sent to Typhoon HIL!")
        else:
            print(f"Failed: {i16ErrorCode} (Detail: {tErrorValue.value})")


        