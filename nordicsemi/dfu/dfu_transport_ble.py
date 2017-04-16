#
# Copyright (c) 2016 Nordic Semiconductor ASA
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
#   1. Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
#   2. Redistributions in binary form must reproduce the above copyright notice, this
#   list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
#
#   3. Neither the name of Nordic Semiconductor ASA nor the names of other
#   contributors to this software may be used to endorse or promote products
#   derived from this software without specific prior written permission.
#
#   4. This software must only be used in or with a processor manufactured by Nordic
#   Semiconductor ASA, or in or with a processor manufactured by a third party that
#   is used in combination with a processor manufactured by Nordic Semiconductor.
#
#   5. Any software provided in binary or object form under this license must not be
#   reverse engineered, decompiled, modified and/or disassembled.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

# Python standard library
import struct
import logging
import binascii

from nordicsemi.dfu.dfu_transport       import DfuTransport, DfuEvent
from pc_ble_driver_py                   import config
from pc_ble_driver_py                   import nrf_types
from pc_ble_driver_py                   import nrf_event
from pc_ble_driver_py.exceptions        import NordicSemiException, IllegalStateException
from pc_ble_driver_py.gattc             import GattClient
from pc_ble_driver_py.nrf_adapter       import NrfAdapter
from pc_ble_driver_py.nrf_driver        import NrfDriver
from pc_ble_driver_py.nrf_event_sync    import EventSync
from pc_ble_driver_py.observers         import NrfAdapterObserver, GattClientObserver

logger  = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)


class ValidationException(NordicSemiException):
    """"
    Exception used when validation failed
    """
    pass


class DFUAdapter(NrfAdapterObserver):
    BASE_UUID       = nrf_types.BLEUUIDBase(
                        [0x8E, 0xC9, 0x00, 0x00, 0xF3, 0x15, 0x4F, 0x60,
                         0x9F, 0xB8, 0x83, 0x88, 0x30, 0xDA, 0xEA, 0x50])
    CP_UUID         = nrf_types.BLEUUID(0x0001, BASE_UUID)
    DP_UUID         = nrf_types.BLEUUID(0x0002, BASE_UUID)
    LOCAL_ATT_MTU   = 23

    def __init__(self, adapter):
        super(DFUAdapter, self).__init__()
        self.adapter    = adapter
        self.cp_handle  = None
        self.cp_uuid    = None
        self.dp_handle  = None
        self.adapter.observer_register(self)

    def open(self):
        self.adapter.driver.open()
        ble_enable_params = nrf_types.BLEEnableParams(
                vs_uuid_count      = 10, service_changed    = False,
                periph_conn_count  =  0, central_conn_count = 1,
                central_sec_count  =  1)
        if config.sd_api_ver_get() >= 3:
            logger.info("\nBLE: ble_enable with local ATT MTU: {}".format(DFUAdapter.LOCAL_ATT_MTU))
            ble_enable_params.att_mtu = DFUAdapter.LOCAL_ATT_MTU

        self.adapter.driver.ble_enable(ble_enable_params)
        self.adapter.driver.ble_vs_uuid_add(DFUAdapter.BASE_UUID)

    def close(self):
        self.adapter.close()

    def _get_device_name(self, event):
        if nrf_types.BLEAdvData.Types.complete_local_name in event.adv_data.records:
            dev_name_list = event.adv_data.records[nrf_types.BLEAdvData.Types.complete_local_name]
            return "".join(chr(e) for e in dev_name_list)
        elif nrf_types.BLEAdvData.Types.short_local_name in event.adv_data.records:
            dev_name_list = event.adv_data.records[nrf_types.BLEAdvData.Types.short_local_name]
            return "".join(chr(e) for e in dev_name_list)
        return ""

    def _get_device_addr(self, event):
        return "".join("{0:02X}".format(b) for b in event.peer_addr.addr)

    def _find_service_handles(self, services):
        cp_uuid     =  DFUAdapter.CP_UUID.base.base[:]
        cp_uuid[2]  = (DFUAdapter.CP_UUID.value >> 8) & 0xff
        cp_uuid[3]  = (DFUAdapter.CP_UUID.value >> 0) & 0xff
        dp_uuid     =  DFUAdapter.DP_UUID.base.base[:]
        dp_uuid[2]  = (DFUAdapter.DP_UUID.value >> 8) & 0xff
        dp_uuid[3]  = (DFUAdapter.DP_UUID.value >> 0) & 0xff
        cccd_uuid   = nrf_types.BLEUUID(nrf_types.BLEUUID.Standard.cccd)

        for service in services:
            for char in service.chars:
                char_data_decl_uuid = list(reversed(char.data_decl[3:])) # TODO: Really?
                if cp_uuid == char_data_decl_uuid:
                    self.cp_handle = char.handle_value
                if dp_uuid == char_data_decl_uuid:
                    self.dp_handle = char.handle_value
                for descr in char.descs:
                    if cp_uuid == char_data_decl_uuid and descr.uuid == cccd_uuid:
                        self.cp_cccd = descr.handle

        if None in [self.cp_handle, self.cp_cccd, self.dp_handle]:
            raise NordicSemiException('Did not find expected services')

    def connect(self, target_device_name, target_device_addr):
        logger.info('BLE: scanning: target address: 0x{}'.format(target_device_addr))

        def scan_for_device(event):
            if not event.evt_id == nrf_event.GapEvtAdvReport.evt_id:
                return True
            if target_device_name and target_device_name == self._get_device_name(event):
                return False
            if target_device_addr and target_device_addr == self._get_device_addr(event):
                return False
            return True

        with EventSync(self.adapter, callback=scan_for_device) as evt_sync:
            self.adapter.driver.ble_gap_scan_start()
            event = evt_sync.get(timeout=5)

            if event is None:
                raise NordicSemiException('Timeout, unable to connect to device.')
            self.adapter.driver.ble_gap_scan_stop()

            self.gattc  = GattClient(self.adapter, event.peer_addr)

        proc_sync = self.gattc.connect().wait(5)
        if not proc_sync.status:
            raise NordicSemiException('Timeout, unable to connect to device.')

        if config.sd_api_ver_get() >= 3:
            if DFUAdapter.LOCAL_ATT_MTU > self.adapter.driver.ATT_MTU_DEFAULT:
                logger.info('BLE: Enabling longer ATT MTUs')
                self.att_mtu = self.adapter.att_mtu_exchange(self.gattc.conn_handle)
                logger.info('BLE: ATT MTU: {}'.format(self.att_mtu))
            else:
                logger.info('BLE: Using default ATT MTU')

        logger.info('BLE: Connected to target, doing service discovery')
        services = self.gattc.get_peer_db()
        self._find_service_handles(services)

        logger.debug('BLE: Enabling Notifications')
        self.gattc.enable_notification(self.cp_cccd)

    def cp_notification_filter(self, event):
        if not event.evt_id == nrf_event.GattcEvtHvx.evt_id:
            return True
        if not self.gattc.conn_handle == event.conn_handle:
            return True
        if not event.attr_handle == self.cp_handle:
            return True
        return False

    def write_control_point(self, data):
        with EventSync(self.adapter, callback=self.cp_notification_filter) as evt_sync:
            self.gattc.write(self.cp_handle, data)
            event = evt_sync.get()
            return event

    def write_data_point(self, data):
        self.gattc.write_cmd(self.dp_handle, data)

    #def on_att_mtu_exchanged(self, ble_driver, conn_handle, att_mtu):
    #    logger.info('ATT MTU exchanged: conn_handle={} att_mtu={}'.format(conn_handle, att_mtu))


    #def on_gattc_evt_exchange_mtu_rsp(self, ble_driver, conn_handle, **kwargs):
    #    logger.info('ATT MTU exchange response: conn_handle={}'.format(conn_handle))



class DfuTransportBle(DfuTransport):

    DATA_PACKET_SIZE    = 20
    DEFAULT_TIMEOUT     = 20
    RETRIES_NUMBER      = 3


    def __init__(self,
                 serial_port,
                 target_device_name=None,
                 target_device_addr=None,
                 baud_rate=1000000,
                 prn=0):
        super(DfuTransportBle, self).__init__()
        self.baud_rate          = baud_rate
        self.serial_port        = serial_port
        self.target_device_name = target_device_name
        self.target_device_addr = target_device_addr
        self.dfu_adapter        = None
        self.prn                = prn

    def open(self):
        if self.dfu_adapter:
            raise IllegalStateException('DFU Adapter is already open')

        super(DfuTransportBle, self).open()
        driver              = NrfDriver(self.serial_port, self.baud_rate)
        self.adapter        = NrfAdapter(driver)
        self.dfu_adapter    = DFUAdapter(self.adapter)
        self.dfu_adapter.open()
        self.dfu_adapter.connect(
                target_device_name = self.target_device_name,
                target_device_addr = self.target_device_addr)
        self.__set_prn()

    def close(self):
        if not self.dfu_adapter:
            return # Silent ignore already closed
            #raise IllegalStateException('DFU Adapter is already closed')
        super(DfuTransportBle, self).close()
        self.dfu_adapter.close()
        self.dfu_adapter = None

    def send_init_packet(self, init_packet):
        def try_to_recover():
            if response['offset'] == 0 or response['offset'] > len(init_packet):
                # There is no init packet or present init packet is too long.
                return False

            expected_crc = (binascii.crc32(init_packet[:response['offset']]) & 0xFFFFFFFF)

            if expected_crc != response['crc']:
                # Present init packet is invalid.
                return False

            if len(init_packet) > response['offset']:
                # Send missing part.
                try:
                    self.__stream_data(data     = init_packet[response['offset']:],
                                       crc      = expected_crc,
                                       offset   = response['offset'])
                except ValidationException:
                    return False

            self.__execute()
            return True

        response = self.__select_command()
        assert len(init_packet) <= response['max_size'], 'Init command is too long'

        if try_to_recover():
            return

        for r in range(DfuTransportBle.RETRIES_NUMBER):
            try:
                self.__create_command(len(init_packet))
                self.__stream_data(data=init_packet)
                self.__execute()
            except ValidationException:
                pass
            break
        else:
            raise NordicSemiException("Failed to send init packet")


    def send_firmware(self, firmware):
        def try_to_recover():
            if response['offset'] == 0:
                # Nothing to recover
                return

            expected_crc = binascii.crc32(firmware[:response['offset']]) & 0xFFFFFFFF
            remainder    = response['offset'] % response['max_size']

            if expected_crc != response['crc']:
                # Invalid CRC. Remove corrupted data.
                response['offset'] -= remainder if remainder != 0 else response['max_size']
                response['crc']     = binascii.crc32(firmware[:response['offset']]) & 0xFFFFFFFF
                return

            if (remainder != 0) and (response['offset'] != len(firmware)):
                # Send rest of the page.
                try:
                    to_send             = firmware[response['offset'] : response['offset'] + response['max_size'] - remainder]
                    response['crc']     = self.__stream_data(data   = to_send,
                                                             crc    = response['crc'],
                                                             offset = response['offset'])
                    response['offset'] += len(to_send)
                except ValidationException:
                    # Remove corrupted data.
                    response['offset'] -= remainder
                    response['crc']     = binascii.crc32(firmware[:response['offset']]) & 0xFFFFFFFF
                    return

            self.__execute()
            self._send_event(event_type=DfuEvent.PROGRESS_EVENT, progress=response['offset'])

        response = self.__select_data()
        try_to_recover()

        for i in range(response['offset'], len(firmware), response['max_size']):
            data = firmware[i:i+response['max_size']]
            for r in range(DfuTransportBle.RETRIES_NUMBER):
                try:
                    self.__create_data(len(data))
                    response['crc'] = self.__stream_data(data=data, crc=response['crc'], offset=i)
                    self.__execute()
                except ValidationException:
                    pass
                break
            else:
                raise NordicSemiException("Failed to send firmware")
            self._send_event(event_type=DfuEvent.PROGRESS_EVENT, progress=len(data))

    def __set_prn(self):
        logger.debug("BLE: Set Packet Receipt Notification {}".format(self.prn))
        event = self.dfu_adapter.write_control_point(
                [DfuTransportBle.OP_CODE['SetPRN']] + map(ord, struct.pack('<H', self.prn)))
        self.__assert_response(event, DfuTransportBle.OP_CODE['SetPRN'])

    def __create_command(self, size):
        self.__create_object(0x01, size)

    def __create_data(self, size):
        self.__create_object(0x02, size)

    def __create_object(self, object_type, size):
        event = self.dfu_adapter.write_control_point(
                [DfuTransportBle.OP_CODE['CreateObject'], object_type] + map(ord, struct.pack('<L', size)))
        self.__assert_response(event, DfuTransportBle.OP_CODE['CreateObject'])

    def __calculate_checksum(self):
        event = self.dfu_adapter.write_control_point([DfuTransportBle.OP_CODE['CalcChecSum']])
        response = self.__assert_response(event, DfuTransportBle.OP_CODE['CalcChecSum'])

        (offset, crc) = struct.unpack('<II', bytearray(response))
        return {'offset': offset, 'crc': crc}

    def __execute(self):
        event = self.dfu_adapter.write_control_point([DfuTransportBle.OP_CODE['Execute']])
        self.__assert_response(event, DfuTransportBle.OP_CODE['Execute'])

    def __select_command(self):
        return self.__select_object(0x01)

    def __select_data(self):
        return self.__select_object(0x02)

    def __select_object(self, object_type):
        logger.debug("BLE: Selecting Object: type:{}".format(object_type))
        event = self.dfu_adapter.write_control_point([DfuTransportBle.OP_CODE['ReadObject'], object_type])
        response = self.__assert_response(event, DfuTransportBle.OP_CODE['ReadObject'])

        (max_size, offset, crc)= struct.unpack('<III', bytearray(response))
        logger.debug("BLE: Object selected: max_size:{} offset:{} crc:{}".format(max_size, offset, crc))
        return {'max_size': max_size, 'offset': offset, 'crc': crc}

    def __get_checksum_response(self, event):
        response = self.__assert_response(event, DfuTransportBle.OP_CODE['CalcChecSum'])

        (offset, crc) = struct.unpack('<II', bytearray(response))
        return {'offset': offset, 'crc': crc}

    def __stream_data(self, data, crc=0, offset=0):
        logger.debug("BLE: Streaming Data: len:{0} offset:{1} crc:0x{2:08X}".format(len(data), offset, crc))
        def validate_crc(response):
            if (crc != response['crc']):
                raise ValidationException('Failed CRC validation.\n'\
                                + 'Expected: {} Recieved: {}.'.format(crc, response['crc']))
            if (offset != response['offset']):
                raise ValidationException('Failed offset validation.\n'\
                                + 'Expected: {} Recieved: {}.'.format(offset, response['offset']))

        current_pnr     = 0
        with EventSync(self.adapter, callback=self.dfu_adapter.cp_notification_filter) as evt_sync:
            for i in range(0, len(data), DfuTransportBle.DATA_PACKET_SIZE):
                to_transmit     = data[i:i + DfuTransportBle.DATA_PACKET_SIZE]
                self.dfu_adapter.write_data_point(map(ord, to_transmit))
                crc     = binascii.crc32(to_transmit, crc) & 0xFFFFFFFF
                offset += len(to_transmit)
                current_pnr    += 1
                if self.prn == current_pnr:
                    current_pnr = 0
                    event = evt_sync.get()
                    response    = self.__get_checksum_response(event)
                    validate_crc(response)

        response = self.__calculate_checksum()
        validate_crc(response)

        return crc


    def __assert_response(self, event, operation):
        def get_dict_key(dictionary, value):
            return next((key for key, val in dictionary.items() if val == value), None)

        if event is None:
            raise NordicSemiException('Timeout: operation - {}'.format(get_dict_key(DfuTransportBle.OP_CODE,
                                                                                    operation)))

        if event.data[0] != DfuTransportBle.OP_CODE['Response']:
            raise NordicSemiException('No Response: 0x{:02X}'.format(event.data[0]))

        if event.data[1] != operation:
            raise NordicSemiException('Unexpected Executed OP_CODE.\n'
                                    + 'Expected: 0x{:02X} Received: 0x{:02X}'.format(operation, event.data[1]))

        if event.data[2] == DfuTransport.RES_CODE['Success']:
            return event.data[3:]
        elif event.data[2] == DfuTransport.RES_CODE['ExtendedError']:
            try:
                data = DfuTransport.EXT_ERROR_CODE[event.data[3]]
            except IndexError:
                data = "Unsupported extended error type {}".format(event.data[3])
            raise NordicSemiException('Extended Error 0x{:02X}: {}'.format(event.data[3], data))
        else:
            raise NordicSemiException('Response Code {}'.format(get_dict_key(DfuTransport.RES_CODE, event.data[2])))

