--!strict
--!optimize 2

-- Modified version of BinBuffer (https://github.com/GameF200/BinBuffer)

export type Buffer = {
	_buffer: buffer,
	_maxSize: number,
	_callback: (buffer: buffer) -> (),
	_flushTask: thread?,
	_destroyed: boolean,
	_writeOffset: number,
	_instances: {Instance},
	_instancesOffset: number,

	Add: (self: Buffer, key: any, value: any) -> boolean,
	Flush: (self: Buffer) -> boolean,
	Destroy: (self: Buffer) -> (),
	Clear: (self: Buffer) -> (),
	AddMultiple: (self: Buffer, ...any) -> ()
}

local Buffer = {}
Buffer.__index = Buffer

local typeof = typeof
local math_max = math.max
local math_min = math.min
local math_floor = math.floor
local math_ceil = math.ceil
local table_insert = table.insert
local table_clear = table.clear
local ipairs = ipairs
local pairs = pairs
local os_clock = os.clock
local task_spawn = task.spawn
local task_cancel = task.cancel
local task_wait = task.wait
local buffer_create = buffer.create
local buffer_copy = buffer.copy
local string_len = string.len

local DATA_TYPES = {
	NIL = 0,
	BOOLEAN = 1,
	NUMBER_I8 = 2,
	NUMBER_I16 = 3,
	NUMBER_I32 = 4,
	NUMBER_U8 = 5,
	NUMBER_U16 = 6,
	NUMBER_U32 = 7,
	NUMBER_F16 = 8,
	NUMBER_F24 = 9,
	NUMBER_F32 = 10,
	NUMBER_F64 = 11,
	STRING = 12,
	STRING_LONG = 13,
	VECTOR2 = 14,
	INSTANCE = 15,
	VECTOR3 = 16,
	COLOR3 = 17,
	UDIM = 18,
	UDIM2 = 19,
	CFRAME = 20,
	RECT = 21,
	NUMBER_RANGE = 22,
	NUMBER_SEQUENCE = 23,
	COLOR_SEQUENCE = 24,
	BRICK_COLOR = 25,
	TABLE = 26,
	KEY_VALUE_PAIR = 27
}

local INT8_MIN = -128
local INT8_MAX = 127
local INT16_MIN = -32768
local INT16_MAX = 32767
local INT32_MIN = -2147483648
local INT32_MAX = 2147483647
local UINT8_MAX = 255
local UINT16_MAX = 65535
local UINT32_MAX = 4294967295

--[[
    Calculate optimal new size for buffer expansion
]]
local function CalculateNewSize(currentSize: number, requiredSize: number): number
	local maxReasonableSize = 64 * 1024 * 1024 -- 64MB

	if requiredSize > maxReasonableSize then
		return requiredSize
	end

	local growthFactor = 1.5

	if currentSize > 1024 * 1024 then -- 1MB
		growthFactor = 1.25
	end
	if currentSize > 10 * 1024 * 1024 then -- 10MB
		growthFactor = 1.1
	end

	local newSize = math_floor(currentSize * growthFactor)
	newSize = math_max(newSize, requiredSize)
	newSize = math_ceil(newSize / 64) * 64

	return math_min(newSize, maxReasonableSize)
end

--[[
    Ensure buffer has enough capacity for new data
]]
local function EnsureCapacity(buf: Buffer, requiredBytes: number): boolean
	local currentBufferSize = buffer.len(buf._buffer)
	local target_len = buf._writeOffset + requiredBytes

	if target_len > buf._maxSize then
		warn(string.format("Buffer size exceeded: required %d bytes, max size: %d bytes", target_len, buf._maxSize))
		return false
	end

	if target_len > currentBufferSize then
		local newSize = CalculateNewSize(currentBufferSize, target_len)

		if newSize < target_len then
			warn("Cannot expand buffer to required size: " .. newSize)
			return false
		end

		local newBuf = buffer_create(newSize)

		if not newBuf then
			warn("Failed to allocate buffer of size: " .. newSize)
			return false
		end

		local bytes_to_copy = buf._writeOffset
		if bytes_to_copy > 0 then
			buffer_copy(newBuf, 0, buf._buffer, 0, bytes_to_copy)
		end

		buf._buffer = newBuf
	end

	return true
end

local WriteF16 = function(buf: Buffer, value: number)
	local bitOffset = buf._writeOffset * 8
	buf._writeOffset += 2

	if value == 0 then
		buffer.writebits(buf._buffer, bitOffset, 16, 0b0_00000_0000000000)
	elseif value >= 65520 then
		buffer.writebits(buf._buffer, bitOffset, 16, 0b0_11111_0000000000)
	elseif value <= -65520 then
		buffer.writebits(buf._buffer, bitOffset, 16, 0b1_11111_0000000000)
	elseif value ~= value then
		buffer.writebits(buf._buffer, bitOffset, 16, 0b0_11111_0000000001)
	else
		local sign = 0
		if value < 0 then sign = 1 value = -value end
		local mantissa, exponent = math.frexp(value)
		buffer.writebits(buf._buffer, bitOffset + 0, 10, mantissa * 2048 - 1023.5)
		buffer.writebits(buf._buffer, bitOffset + 10, 5, exponent + 14)
		buffer.writebits(buf._buffer, bitOffset + 15, 1, sign)
	end
end

local WriteF24 = function(buf: Buffer, value: number)
	local bitOffset = buf._writeOffset * 8
	buf._writeOffset += 3

	if value == 0 then
		buffer.writebits(buf._buffer, bitOffset, 24, 0b0_000000_00000000000000000) 
	elseif value >= 4294959104 then
		buffer.writebits(buf._buffer, bitOffset, 24, 0b0_111111_00000000000000000)
	elseif value <= -4294959104 then
		buffer.writebits(buf._buffer, bitOffset, 24, 0b1_111111_00000000000000000)
	elseif value ~= value then
		buffer.writebits(buf._buffer, bitOffset, 24, 0b0_111111_00000000000000001)
	else
		local sign = 0
		if value < 0 then sign = 1 value = -value end
		local mantissa, exponent = math.frexp(value)
		buffer.writebits(buf._buffer, bitOffset + 0, 17, mantissa * 262144 - 131071.5)
		buffer.writebits(buf._buffer, bitOffset + 17, 6, exponent + 30)
		buffer.writebits(buf._buffer, bitOffset + 23, 1, sign)
	end
end

local WriteF32 = function(buf: Buffer, value: number)
	buffer.writef32(buf._buffer, buf._writeOffset, value)
	buf._writeOffset += 4
end

local WriteF64 = function(buf: Buffer, value: number)
	buffer.writef64(buf._buffer, buf._writeOffset, value)
	buf._writeOffset += 8
end

local WriteI8 = function(buf: Buffer, value: number)
	buffer.writei8(buf._buffer, buf._writeOffset, value)
	buf._writeOffset += 1
end

local WriteI16 = function(buf: Buffer, value: number)
	buffer.writei16(buf._buffer, buf._writeOffset, value)
	buf._writeOffset += 2
end

local WriteI32 = function(buf: Buffer, value: number)
	buffer.writei32(buf._buffer, buf._writeOffset, value)
	buf._writeOffset += 4
end

local WriteU8 = function(buf: Buffer, value: number)
	buffer.writeu8(buf._buffer, buf._writeOffset, value)
	buf._writeOffset += 1
end

local WriteU16 = function(buf: Buffer, value: number)
	buffer.writeu16(buf._buffer, buf._writeOffset, value)
	buf._writeOffset += 2
end

local WriteU32 = function(buf: Buffer, value: number)
	buffer.writeu32(buf._buffer, buf._writeOffset, value)
	buf._writeOffset += 4
end

local WriteString = function(buf, value: string)
	buffer.writestring(buf._buffer, buf._writeOffset, value)
	buf._writeOffset += #value
end

--[[
    Classify number type for optimal storage
]]
local function ClassifyNumber(value: number): number
	if value == value // 1 then -- note: value % 1 == 0 reduced perfomance by 8%
		if value >= 0 then
			if value <= UINT8_MAX then return 5 -- TYPE_UINT8
			elseif value <= UINT16_MAX then return 6 -- TYPE_UINT16
			elseif value <= UINT32_MAX then return 7 -- TYPE_UINT32
			else return 11 end -- TYPE_F64
		else
			if value >= INT8_MIN then return 2 -- TYPE_INT8
			elseif value >= INT16_MIN then return 3 -- TYPE_INT16
			elseif value >= INT32_MIN then return 4 -- TYPE_INT32
			else return 11 end -- TYPE_F64
		end
	else
		local absValue = math.abs(value)
		if absValue <= 65520 then return 8 -- TYPE_F16
		elseif absValue <= 4294959104 then return 9 -- TYPE_F24
		elseif absValue <= 3.4028235e38 then return 10 -- TYPE_F32
		else return 11 end -- TYPE_F64
	end
end

local NUMBER_WRITERS = {
	[1] = function(buf: Buffer, value: number) -- TYPE_INT8
		if not EnsureCapacity(buf, 2) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_I8)
		WriteI8(buf, value)
		return true
	end,

	[2] = function(buf: Buffer, value: number) -- TYPE_INT16
		if not EnsureCapacity(buf, 3) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_I16)
		WriteI16(buf, value)
		return true
	end,

	[3] = function(buf: Buffer, value: number) -- TYPE_INT32
		if not EnsureCapacity(buf, 5) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_I32)
		WriteI32(buf, value)
		return true
	end,

	[4] = function(buf: Buffer, value: number) -- TYPE_UINT8
		if not EnsureCapacity(buf, 2) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_U8)
		WriteU8(buf, value)
		return true
	end,

	[5] = function(buf: Buffer, value: number) -- TYPE_UINT16
		if not EnsureCapacity(buf, 3) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_U16)
		WriteU16(buf, value)
		return true
	end,

	[6] = function(buf: Buffer, value: number) -- TYPE_UINT32
		if not EnsureCapacity(buf, 5) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_U32)
		WriteU32(buf, value)
		return true
	end,

	[7] = function(buf: Buffer, value: number) -- TYPE_F16
		if not EnsureCapacity(buf, 3) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_F16)
		WriteF16(buf, value)
		return true
	end,

	[8] = function(buf: Buffer, value: number) -- TYPE_F24
		if not EnsureCapacity(buf, 4) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_F24)
		WriteF24(buf, value)
		return true
	end,

	[9] = function(buf: Buffer, value: number) -- TYPE_F32
		if not EnsureCapacity(buf, 5) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_F32)
		WriteF32(buf, value)
		return true
	end,

	[10] = function(buf: Buffer, value: number) -- TYPE_DOUBLE (F64)
		if not EnsureCapacity(buf, 9) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_F64)
		WriteF64(buf, value)
		return true
	end,
}

--[[
    Write number with automatic type classification
]]
local function WriteNumber(buf: Buffer, value: number)
	local numType = ClassifyNumber(value)
	

	if numType < 1 or numType > 10 then
		return false
	end

	local writer = NUMBER_WRITERS[numType]
	if not writer then
		return false
	end

	return writer(buf, value)
end

--[[
    Write string data
    (short or long)
]]
local function WriteStringData(buf: Buffer, value: string)
	local len = string_len(value)
	if len < 256 then
		if not EnsureCapacity(buf, 2 + len) then return false end
		WriteU8(buf, DATA_TYPES.STRING)
		WriteU8(buf, len)
		WriteString(buf, value)
		return true
	else
		if not EnsureCapacity(buf, 3 + len) then return false end
		WriteU8(buf, DATA_TYPES.STRING_LONG)
		WriteU16(buf, len)
		WriteString(buf, value)
		return true
	end
end

local TYPE_WRITERS = {
	boolean = function(buf: Buffer, value: boolean)
		if not EnsureCapacity(buf, 2) then return false end
		WriteU8(buf, DATA_TYPES.BOOLEAN)
		WriteU8(buf, value and 1 or 0)
		return true
	end,

	number = WriteNumber,

	string = WriteStringData,

	Vector3 = function(buf: Buffer, value: Vector3)
		if not EnsureCapacity(buf, 13) then return false end
		WriteU8(buf, DATA_TYPES.VECTOR3)
		WriteF32(buf, value.X)
		WriteF32(buf, value.Y)
		WriteF32(buf, value.Z)
		return true
	end,

	Vector2 = function(buf: Buffer, value: Vector2)
		if not EnsureCapacity(buf, 9) then return false end
		WriteU8(buf, DATA_TYPES.VECTOR2)
		WriteF32(buf, value.X)
		WriteF32(buf, value.Y)
		return true
	end,

	CFrame = function(buf: Buffer, value: CFrame)
		if not EnsureCapacity(buf, 19) then return false end
		local rx, ry, rz = value:ToEulerAnglesXYZ()
		WriteU8(buf, DATA_TYPES.CFRAME)
		WriteU16(buf, rx * 10430.219195527361 + 0.5)
		WriteU16(buf, ry * 10430.219195527361 + 0.5)
		WriteU16(buf, rz * 10430.219195527361 + 0.5)
		WriteF32(buf, value.X)
		WriteF32(buf, value.Y)
		WriteF32(buf, value.Z)
		return true    
	end,

	Color3 = function(buf: Buffer, value: Color3)
		if not EnsureCapacity(buf, 4) then return false end
		WriteU8(buf, DATA_TYPES.COLOR3)
		WriteU8(buf, value.R * 255 + 0.5)
		WriteU8(buf, value.G * 255 + 0.5)
		WriteU8(buf, value.B * 255 + 0.5)
		return true
	end,

	UDim = function(buf: Buffer, value: UDim)
		if not EnsureCapacity(buf, 5) then return false end
		WriteU8(buf, DATA_TYPES.UDIM)
		WriteI16(buf, value.Scale * 1000)
		WriteI16(buf, value.Offset)
		return true 
	end,

	UDim2 = function(buf: Buffer, value: UDim2)
		if not EnsureCapacity(buf, 9) then return false end
		WriteU8(buf, DATA_TYPES.UDIM2)
		WriteI16(buf, value.X.Scale * 1000)
		WriteI16(buf, value.X.Offset)
		WriteI16(buf, value.Y.Scale * 1000)
		WriteI16(buf, value.Y.Offset)
		return true
	end,

	Rect = function(buf: Buffer, value: Rect)
		if not EnsureCapacity(buf, 17) then return false end
		WriteU8(buf, DATA_TYPES.RECT)
		WriteF32(buf, value.Min.X)
		WriteF32(buf, value.Min.Y)
		WriteF32(buf, value.Max.X)
		WriteF32(buf, value.Max.Y)
		return true
	end,

	NumberRange = function(buf: Buffer, value: NumberRange)
		if not EnsureCapacity(buf, 9) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_RANGE)
		WriteF32(buf, value.Min)
		WriteF32(buf, value.Max)
		return true
	end,

	NumberSequence = function(buf: Buffer, value: NumberSequence)
		local len = #value.Keypoints
		if not EnsureCapacity(buf, 2 + len * 3) then return false end
		WriteU8(buf, DATA_TYPES.NUMBER_SEQUENCE)
		WriteU8(buf, len)
		for _, keypoint in ipairs(value.Keypoints) do
			WriteU8(buf, keypoint.Time * 255 + 0.5)
			WriteU8(buf, keypoint.Value * 255 + 0.5)
			WriteU8(buf, keypoint.Envelope * 255 + 0.5)
		end
		return true
	end,

	ColorSequence = function(buf: Buffer, value: ColorSequence)
		local len = #value.Keypoints
		if not EnsureCapacity(buf, 2 + len * 4) then return false end
		WriteU8(buf, DATA_TYPES.COLOR_SEQUENCE)
		WriteU8(buf, len)
		for _, keypoint in ipairs(value.Keypoints) do
			WriteU8(buf, keypoint.Time * 255 + 0.5)
			WriteU8(buf, keypoint.Value.R * 255 + 0.5)
			WriteU8(buf, keypoint.Value.G * 255 + 0.5)
			WriteU8(buf, keypoint.Value.B * 255 + 0.5)
		end
		return true
	end,

	BrickColor = function(buf: Buffer, value: BrickColor)
		if not EnsureCapacity(buf, 3) then return false end
		WriteU8(buf, DATA_TYPES.BRICK_COLOR)
		WriteU16(buf, value.Number)
		return true
	end,

	Instance = function(buf: Buffer, value: Instance)
		if not EnsureCapacity(buf, 1) then return false end
		WriteU8(buf, DATA_TYPES.INSTANCE)
		buf._instancesOffset += 1
		buf._instances[buf._instancesOffset] = value
		return true
	end,

	table = function(buf: Buffer, value: {[any]: any})
		if not EnsureCapacity(buf, 1) then return false end
		WriteU8(buf, DATA_TYPES.TABLE)

		for key, val in pairs(value) do
			if not Buffer._writeData(buf, key) then return false end
			if not Buffer._writeData(buf, val) then return false end
		end

		if not EnsureCapacity(buf, 1) then return false end
		WriteU8(buf, DATA_TYPES.NIL)
		return true
	end,
}

--[[
    Internal data writing function
]]
function Buffer._writeData(buf: Buffer, data: any)
	local dataType = typeof(data)
	local writer = TYPE_WRITERS[dataType]

	if writer then
		return writer(buf, data)
	else
		-- Fallback for unknown types
		return WriteStringData(buf, tostring(data))
	end
end

--[[
    Add data to buffer (single value or key-value pair)
]]
function Buffer:Add(...): boolean
	if self._destroyed then 
		return false 
	end

	local args = {...}
	local count = select("#", ...)


	if count == 1 then
		local result = Buffer._writeData(self, args[1])
		return result
	elseif count == 2 then
		if not EnsureCapacity(self, 1) then 
			return false 
		end

		WriteU8(self, DATA_TYPES.KEY_VALUE_PAIR)
		
		local keyResult = Buffer._writeData(self, args[1])
		
		local valueResult = Buffer._writeData(self, args[2])
	
		return keyResult and valueResult
	else
		warn("Buffer:Add expects 1 or 2 arguments")
		return false
	end
end

function Buffer:AddMultiple(...)
	for i = 1, select("#", ...) do
		self:Add(select(i, ...))
	end
end
function Buffer:Tostring(): string
	if self._destroyed then
		return ""
	end

	local serializationBuffer = Buffer.create({
		size = self._writeOffset + 100,
		callback = function() end
	})

	if not EnsureCapacity(serializationBuffer, 1) then return "" end
	WriteU8(serializationBuffer, 1) 

	if not EnsureCapacity(serializationBuffer, 4) then return "" end
	WriteU32(serializationBuffer, self._writeOffset)

	if self._writeOffset > 0 then
		if not EnsureCapacity(serializationBuffer, 4 + self._writeOffset) then return "" end
		WriteU32(serializationBuffer, self._writeOffset) 
		for i = 0, self._writeOffset - 1 do
			WriteU8(serializationBuffer, buffer.readu8(self._buffer, i))
		end
	else
		WriteU32(serializationBuffer, 0)
	end

	if not EnsureCapacity(serializationBuffer, 4) then return "" end
	WriteU32(serializationBuffer, self._maxSize)

	return Buffer._ToBase64(serializationBuffer)
end

function Buffer._ToBase64(buf: Buffer): string
	local base64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	local result = {}
	local resultIndex = 1

	local bufferData = buf._buffer
	local dataLength = buf._writeOffset

	for i = 0, dataLength - 1, 3 do
		local byte1 = buffer.readu8(bufferData, i) or 0
		local byte2 = buffer.readu8(bufferData, i + 1) or 0
		local byte3 = buffer.readu8(bufferData, i + 2) or 0

		local combined = bit32.lshift(byte1, 16) + bit32.lshift(byte2, 8) + byte3

		local group1 = bit32.rshift(combined, 18) % 64
		local group2 = bit32.rshift(combined, 12) % 64
		local group3 = bit32.rshift(combined, 6) % 64
		local group4 = combined % 64

		result[resultIndex] = string.sub(base64Chars, group1 + 1, group1 + 1)
		result[resultIndex + 1] = string.sub(base64Chars, group2 + 1, group2 + 1)
		result[resultIndex + 2] = string.sub(base64Chars, group3 + 1, group3 + 1)
		result[resultIndex + 3] = string.sub(base64Chars, group4 + 1, group4 + 1)

		resultIndex += 4
	end

	local padding = dataLength % 3
	if padding == 1 then
		result[#result - 1] = "="
		result[#result] = "="
	elseif padding == 2 then
		result[#result] = "="
	end

	return table.concat(result)
end

function Buffer._FromBase64(base64String: string): Buffer?
	if typeof(base64String) ~= "string" or #base64String == 0 then
		return nil
	end

	base64String = string.gsub(base64String, "%s", "")

	if not string.match(base64String, "^[A-Za-z0-9+/]*=*$") then
		warn("Invalid Base64 string")
		return nil
	end

	local base64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	local charToValue = {}

	for i = 1, 64 do
		charToValue[string.sub(base64Chars, i, i)] = i - 1
	end

	local buffer = Buffer.create({
		size = math.ceil(#base64String * 0.75),
		callback = function() end
	})

	local dataLength = #base64String
	local padding = 0
	if string.sub(base64String, -2) == "==" then
		padding = 2
	elseif string.sub(base64String, -1) == "=" then
		padding = 1
	end

	for i = 1, dataLength - padding, 4 do
		local char1 = string.sub(base64String, i, i)
		local char2 = string.sub(base64String, i + 1, i + 1)
		local char3 = string.sub(base64String, i + 2, i + 2)
		local char4 = string.sub(base64String, i + 3, i + 3)

		local val1 = charToValue[char1] or 0
		local val2 = charToValue[char2] or 0
		local val3 = charToValue[char3] or 0
		local val4 = charToValue[char4] or 0

		local combined = bit32.lshift(val1, 18) + bit32.lshift(val2, 12) + bit32.lshift(val3, 6) + val4

		local byte1 = bit32.rshift(combined, 16) % 256
		local byte2 = bit32.rshift(combined, 8) % 256
		local byte3 = combined % 256

		if not EnsureCapacity(buffer, 3) then
			buffer:Destroy()
			return nil
		end

		WriteU8(buffer, byte1)

		if i + 1 <= dataLength - padding then
			WriteU8(buffer, byte2)
		end
		if i + 2 <= dataLength - padding then
			WriteU8(buffer, byte3)
		end
	end

	return buffer
end

function Buffer.Fromstring(encodedString: string): Buffer?
	if typeof(encodedString) ~= "string" or #encodedString == 0 then
		return nil
	end

	local serializationBuffer = Buffer._FromBase64(encodedString)
	if not serializationBuffer then
		return nil
	end

	local offset = 0

	local function ReadU8(): number
		local value = buffer.readu8(serializationBuffer._buffer, offset)
		offset += 1
		return value
	end

	local function ReadU32(): number
		local value = buffer.readu32(serializationBuffer._buffer, offset)
		offset += 4
		return value
	end

	local function ReadBytes(length: number): {number}
		local bytes = {}
		for i = 1, length do
			bytes[i] = buffer.readu8(serializationBuffer._buffer, offset)
			offset += 1
		end
		return bytes
	end

	local function ReadString(): string
		local length = ReadU8()
		return buffer.readstring(serializationBuffer._buffer, offset, length)
	end

	local version = ReadU8()
	if version ~= 1 then
		warn("Unsupported buffer serialization version: " .. version)
		return nil
	end

	local writeOffset = ReadU32()
	local dataLength = ReadU32()

	local newBuffer = Buffer.create({
		size = math.max(32, dataLength),
		callback = function() end
	})

	if dataLength > 0 then
		local bytes = ReadBytes(dataLength)
		for i, byte in ipairs(bytes) do
			if not EnsureCapacity(newBuffer, 1) then
				newBuffer:Destroy()
				return nil
			end
			WriteU8(newBuffer, byte)
		end
		newBuffer._writeOffset = writeOffset
	end

	newBuffer._maxSize = ReadU32()

	serializationBuffer:Destroy()

	return newBuffer
end


--[[
    Flush buffer contents to callback
]]
function Buffer:Flush(): boolean
	if self._writeOffset == 0 or self._destroyed then 
		return false 
	end

	local actualDataSize = math_min(self._writeOffset, buffer.len(self._buffer))
	if actualDataSize == 0 then
		return false
	end

	local filled = buffer_create(actualDataSize)
	buffer_copy(filled, 0, self._buffer, 0, actualDataSize)

	if self._callback then
		self._callback(filled)
	end

	self:Clear()

	return true
end



--[[
    Clear buffer contents without destroying it
]]
function Buffer:Clear()
	if self._destroyed then return end

	self._writeOffset = 0
	self._instancesOffset = 0
	table_clear(self._instances)

	local currentSize = buffer.len(self._buffer)
	if currentSize > 1024 * 1024 and currentSize > self._maxSize * 4 then
		self._buffer = buffer_create(self._maxSize)
	end
end

--[[
    Destroy buffer and cleanup resources
]]
function Buffer:Destroy()
	if self._destroyed then return end

	self._destroyed = true

	if self._flushTask then
		task_cancel(self._flushTask)
		self._flushTask = nil
	end

	self._buffer = nil
	self._writeOffset = 0
	self._instancesOffset = 0
	table_clear(self._instances)
end

-- Simple utility functions
function Buffer.bytes(bytes: number)
	return bytes
end

function Buffer.kilobytes(kilobytes: number)
	return kilobytes * 1024
end

function Buffer.megabytes(megabytes: number)
	return Buffer.kilobytes(megabytes * 1024)
end

--[[
	Reads a Buffer object and returns all data in the buffer as a table with keys
]]
function Buffer.Read(buf: Buffer): ({[any]: any})
	local result = {}
	local offset = 0
	local instancesOffset = 0

	local function ReadS8(): number local value = buffer.readi8(buf._buffer, offset) offset += 1 return value end
	local function ReadS16(): number local value = buffer.readi16(buf._buffer, offset) offset += 2 return value end
	local function ReadS32(): number local value = buffer.readi32(buf._buffer, offset) offset += 4 return value end
	local function ReadU8(): number local value = buffer.readu8(buf._buffer, offset) offset += 1 return value end
	local function ReadU16(): number local value = buffer.readu16(buf._buffer, offset) offset += 2 return value end
	local function ReadU32(): number local value = buffer.readu32(buf._buffer, offset) offset += 4 return value end
	local function ReadF32(): number local value = buffer.readf32(buf._buffer, offset) offset += 4 return value end
	local function ReadF64(): number local value = buffer.readf64(buf._buffer, offset) offset += 8 return value end
	local function ReadString(length: number): string local value = buffer.readstring(buf._buffer, offset, length) offset += length return value end

	local function ReadF16(): number
		local bitOffset = offset * 8
		offset += 2
		local mantissa = buffer.readbits(buf._buffer, bitOffset + 0, 10)
		local exponent = buffer.readbits(buf._buffer, bitOffset + 10, 5)
		local sign = buffer.readbits(buf._buffer, bitOffset + 15, 1)
		if mantissa == 0 then
			if exponent == 0 then return 0 end
			if exponent == 31 then return if sign == 0 then math.huge else -math.huge end
		elseif exponent == 31 then return 0/0 end
		if sign == 0 then
			return (mantissa / 1024 + 1) * 2 ^ (exponent - 15)
		else
			return -(mantissa / 1024 + 1) * 2 ^ (exponent - 15)
		end
	end

	local function ReadF24(): number
		local bitOffset = offset * 8
		offset += 3
		local mantissa = buffer.readbits(buf._buffer, bitOffset + 0, 17)
		local exponent = buffer.readbits(buf._buffer, bitOffset + 17, 6)
		local sign = buffer.readbits(buf._buffer, bitOffset + 23, 1)
		if mantissa == 0 then
			if exponent == 0 then return 0 end
			if exponent == 63 then return if sign == 0 then math.huge else -math.huge end
		elseif exponent == 63 then return 0/0 end
		if sign == 0 then
			return (mantissa / 131072 + 1) * 2 ^ (exponent - 31)
		else
			return -(mantissa / 131072 + 1) * 2 ^ (exponent - 31)
		end
	end

	local DATA_READERS
	DATA_READERS = {
		[DATA_TYPES.NIL] = function() return nil end,
		[DATA_TYPES.BOOLEAN] = function() return ReadU8() == 1 end,
		[DATA_TYPES.NUMBER_I8] = ReadS8,
		[DATA_TYPES.NUMBER_I16] = ReadS16,
		[DATA_TYPES.NUMBER_I32] = ReadS32,
		[DATA_TYPES.NUMBER_U8] = ReadU8,
		[DATA_TYPES.NUMBER_U16] = ReadU16,
		[DATA_TYPES.NUMBER_U32] = ReadU32,
		[DATA_TYPES.NUMBER_F16] = ReadF16,
		[DATA_TYPES.NUMBER_F24] = ReadF24,
		[DATA_TYPES.NUMBER_F32] = ReadF32,
		[DATA_TYPES.NUMBER_F64] = ReadF64,
		[DATA_TYPES.STRING] = function() return ReadString(ReadU8()) end,
		[DATA_TYPES.STRING_LONG] = function() return ReadString(ReadU16()) end,
		[DATA_TYPES.VECTOR2] = function() return Vector2.new(ReadF32(), ReadF32()) end,
		[DATA_TYPES.VECTOR3] = function() return Vector3.new(ReadF32(), ReadF32(), ReadF32()) end,
		[DATA_TYPES.COLOR3] = function() return Color3.fromRGB(ReadU8(), ReadU8(), ReadU8()) end,
		[DATA_TYPES.UDIM] = function() return UDim.new(ReadS16() / 1000, ReadS16()) end,
		[DATA_TYPES.UDIM2] = function() return UDim2.new(ReadS16() / 1000, ReadS16(), ReadS16() / 1000, ReadS16()) end,
		[DATA_TYPES.CFRAME] = function()
			local rx = ReadU16() / 10430.219195527361
			local ry = ReadU16() / 10430.219195527361
			local rz = ReadU16() / 10430.219195527361
			return CFrame.fromEulerAnglesXYZ(rx, ry, rz) + Vector3.new(ReadF32(), ReadF32(), ReadF32())
		end,
		[DATA_TYPES.RECT] = function() return Rect.new(ReadF32(), ReadF32(), ReadF32(), ReadF32()) end,
		[DATA_TYPES.NUMBER_RANGE] = function() return NumberRange.new(ReadF32(), ReadF32()) end,
		[DATA_TYPES.NUMBER_SEQUENCE] = function()
			local length = ReadU8()
			local keypoints = {}
			for i = 1, length do
				table_insert(keypoints, NumberSequenceKeypoint.new(
					ReadU8() / 255,
					ReadU8() / 255,
					ReadU8() / 255
					))
			end
			return NumberSequence.new(keypoints)
		end,
		[DATA_TYPES.COLOR_SEQUENCE] = function()
			local length = ReadU8()
			local keypoints = {}
			for i = 1, length do
				table_insert(keypoints, ColorSequenceKeypoint.new(
					ReadU8() / 255,
					Color3.fromRGB(ReadU8(), ReadU8(), ReadU8())
					))
			end
			return ColorSequence.new(keypoints)
		end,
		[DATA_TYPES.BRICK_COLOR] = function() return BrickColor.new(ReadU16()) end,
		[DATA_TYPES.TABLE] = function()
			local tbl = {}
			while offset < buf._writeOffset do
				local keyType = ReadU8()
				if keyType == DATA_TYPES.NIL then break end

				local keyReader = DATA_READERS[keyType]
				if not keyReader then
					warn("Unknown key type in table: " .. keyType)
					break
				end
				local key = keyReader()

				local valueType = ReadU8()
				local valueReader = DATA_READERS[valueType]
				if not valueReader then
					warn("Unknown value type in table: " .. valueType)
					break
				end
				local value = valueReader()

				tbl[key] = value
			end
			return tbl
		end,
		
	}

	local function ReadData(): any
		local dataType = ReadU8()
		local reader = DATA_READERS[dataType]
		return reader and reader() or nil
	end

	local index = 1

	while offset < buf._writeOffset do
		local dataType = ReadU8()
		if dataType == DATA_TYPES.KEY_VALUE_PAIR then

			local key = ReadData()
			local value = ReadData()

			if key ~= nil then
				result[key] = value
			end
		else
		
			local reader = DATA_READERS[dataType]
			if reader then
				local value = reader()
				result[index] = value
				index += 1
			else
				warn("Unknown data type while reading: " .. dataType)
				break
			end
		end
	end

	return result
end

function Buffer.create(options: {
	size: number?, 
	callback: (buf: buffer) -> ()?
	}): Buffer
	local size = options and options.size or 32
	local maxSize = Buffer.megabytes(32)
	local callback = options and options.callback or function() end

	local bufferObj = buffer_create(size)

	local self = setmetatable({
		_buffer = bufferObj,
		_callback = callback,
		_initialSize = size,
		_maxSize = maxSize,
		_flushTask = nil,
		_destroyed = false,
		_writeOffset = 0, 
		_instances = {},
		_instancesOffset = 0,
	}, Buffer)

	return self
end

return setmetatable(Buffer, {
	__call = function(_, options)
		return Buffer.create(options)
	end,
})