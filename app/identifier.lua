local M = {}

local digest = require('digest')
local msgpack = require('msgpack')
M.cmp_data = {}

local Identifier = {}

function Identifier:new(data)
    local obj = {}
    obj.hash = digest.md5_hex(msgpack.encode(data))
    obj.data = data
    return obj
end

function M.phone_number(phone_number)
    local data = {'phone_number', phone_number}
    return Identifier:new(data)
end

function M.cmp_data.phone_number(data_one, data_two)
    return #data_one == 2 and #data_two == 2 and (data_one[1] == data_two[1]) and (data_one[2] == data_two[2] ~= nil)
end

return M
