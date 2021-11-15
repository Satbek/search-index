local M = {}

local errors = require('errors')

M.cmp_data = require('app.identifier').cmp_data
M.user_id = {}

local log = require('log')
local json = require('json')

local not_found_err = errors.new_class('SEARCH_STORAGE: NOT_FOUND')

function M.user_id.get_by_phone_number(hash, data)
    local result = {}
    for _, t in box.space.user_search_index.index.hash:pairs({hash}, 'EQ') do
        if t.data_hash ~= hash then
            break
        end
        if M.cmp_data.phone_number(t.data, data) then
            result[#result + 1] = t.user_id
        end
    end
    if #result == 0 then
        return nil, not_found_err:new()
    end
    return result
end

function M.user_id.add_phone_number_identifier(user_id, hash, data, bucket_id)
    box.space.user_search_index:replace({
        user_id,
        bucket_id,
        hash,
        data
    })

    return true
end

return M
