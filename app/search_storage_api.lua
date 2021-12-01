local M = {}

local errors = require('errors')

M.cmp_data = require('app.identifier').cmp_data
M.user_id = {}

local log = require('log')
local json = require('json')

local not_found_err = errors.new_class('SEARCH_STORAGE: NOT_FOUND')
local identifier_err = errors.new_class('SEARCH_STORAGE: IDENTIFIER')

local function add_identifier(user_id, hash, data, bucket_id)
    box.space.user_search_index:replace({
        user_id,
        bucket_id,
        hash,
        data
    })
end

local function delete_identifier(user_id, hash)
    box.space.user_search_index:delete({
        user_id,
        hash,
    })
end

local function get_users_by_hash(hash, data, cmp_func)
    local result = {}
    for _, t in box.space.user_search_index.index.hash:pairs({hash}, 'EQ') do
        if t.data_hash ~= hash then
            break
        end
        if cmp_func(t.data, data) then
            result[#result + 1] = t.user_id
        end
    end
    if #result == 0 then
        return nil, not_found_err:new()
    end
    return result
end

function M.user_id.get_by_phone_number(hash, data)
    return get_users_by_hash(hash, data, M.cmp_data.phone_number)
end

function M.user_id.add_phone_number_identifier(user_id, hash, data, bucket_id)
    local ok, err = pcall(add_identifier, user_id, hash, data, bucket_id)
    if not ok then
        return identifier_err:new('add_phone_number_identifier: ' .. err)
    end
    return ok
end

function M.user_id.get_by_email(hash, data)
    return get_users_by_hash(hash, data, M.cmp_data.email)
end

function M.user_id.add_email_identifier(user_id, hash, data, bucket_id)
    local ok, err = pcall(add_identifier, user_id, hash, data, bucket_id)
    if not ok then
        return identifier_err:new('add_email_identifier: ' .. err)
    end
    return ok
end

function M.user_id.get_by_passport_num(hash, data)
    return get_users_by_hash(hash, data, M.cmp_data.passport_num)
end

function M.user_id.add_passport_num_identifier(user_id, hash, data, bucket_id)
    local ok, err = pcall(add_identifier, user_id, hash, data, bucket_id)
    if not ok then
        return identifier_err:new('add_passport_num_identifier: ' .. err)
    end
    return ok
end

function M.user_id.get_by_geoposition(hash, data)
    return get_users_by_hash(hash, data, M.cmp_data.geoposition)
end

function M.user_id.add_geoposition_identifier(user_id, hash, data, bucket_id)
    local ok, err = pcall(add_identifier, user_id, hash, data, bucket_id)
    if not ok then
        return identifier_err:new('add_geoposition_identifier: ' .. err)
    end
    return ok
end

function M.user_id.get_by_phone_number_hash(hash, data)
    return get_users_by_hash(hash, data, M.cmp_data.phone_number_hash)
end

function M.user_id.add_phone_number_hash_identifier(user_id, hash, data, bucket_id)
    local ok, err = pcall(add_identifier, user_id, hash, data, bucket_id)
    if not ok then
        return identifier_err:new('add_geoposition_identifier: ' .. err)
    end
    return ok
end

function M.user_id.delete_phone_number_identifier(user_id, hash)
    local ok, err = pcall(delete_identifier, user_id, hash)
    if not ok then
        return identifier_err:new('delete_phone_number_identifier: ' .. err)
    end
    return ok
end

function M.user_id.delete_phone_number_hash_identifier(user_id, hash)
    local ok, err = pcall(delete_identifier, user_id, hash)
    if not ok then
        return identifier_err:new('delete_phone_number_hash_identifier: ' .. err)
    end
    return ok
end

return M
