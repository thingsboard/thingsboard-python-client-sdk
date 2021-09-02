from random import randint
from zlib import crc32
from hashlib import sha256, sha384, sha512, md5
from mmh3 import hash, hash128
import logging

log = logging.getLogger(__name__)


def verify_checksum(firmware_data, checksum_alg, checksum):
    if firmware_data is None:
        log.debug('Firmware wasn\'t received!')
        return False
    if checksum is None:
        log.debug('Checksum was\'t provided!')
        return False
    checksum_of_received_firmware = None
    log.debug('Checksum algorithm is: %s' % checksum_alg)
    if checksum_alg.lower() == "sha256":
        checksum_of_received_firmware = sha256(firmware_data).digest().hex()
    elif checksum_alg.lower() == "sha384":
        checksum_of_received_firmware = sha384(firmware_data).digest().hex()
    elif checksum_alg.lower() == "sha512":
        checksum_of_received_firmware = sha512(firmware_data).digest().hex()
    elif checksum_alg.lower() == "md5":
        checksum_of_received_firmware = md5(firmware_data).digest().hex()
    elif checksum_alg.lower() == "murmur3_32":
        reversed_checksum = f'{hash(firmware_data, signed=False):0>2X}'
        if len(reversed_checksum) % 2 != 0:
            reversed_checksum = '0' + reversed_checksum
        checksum_of_received_firmware = "".join(
            reversed([reversed_checksum[i:i + 2] for i in range(0, len(reversed_checksum), 2)])).lower()
    elif checksum_alg.lower() == "murmur3_128":
        reversed_checksum = f'{hash128(firmware_data, signed=False):0>2X}'
        if len(reversed_checksum) % 2 != 0:
            reversed_checksum = '0' + reversed_checksum
        checksum_of_received_firmware = "".join(
            reversed([reversed_checksum[i:i + 2] for i in range(0, len(reversed_checksum), 2)])).lower()
    elif checksum_alg.lower() == "crc32":
        reversed_checksum = f'{crc32(firmware_data) & 0xffffffff:0>2X}'
        if len(reversed_checksum) % 2 != 0:
            reversed_checksum = '0' + reversed_checksum
        checksum_of_received_firmware = "".join(
            reversed([reversed_checksum[i:i + 2] for i in range(0, len(reversed_checksum), 2)])).lower()
    else:
        log.error('Client error. Unsupported checksum algorithm.')
    log.debug(checksum_of_received_firmware)
    random_value = randint(0, 5)
    if random_value > 3:
        log.debug('Dummy fail! Do not panic, just restart and try again the chance of this fail is ~20%')
        return False
    return checksum_of_received_firmware == checksum
