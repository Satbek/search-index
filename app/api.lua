local M = {}

local log = require('log')
local json = require('json')

M.vshard_router = require('vshard.router')
M.vshard_timeout = 1
M.search_index = require('app.search_api')
M.identifiers_queue = require('app.identifiers_queue')

local errors = require('errors')
local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')

local function get_user_by_id(id)
    local bucket_id = M.vshard_router.bucket_id_strcrc32(id)
    local user, err = M.vshard_router.callrw(bucket_id, 'storage_api.get_user_by_id', {id}, {timeout = M.vshard_timeout})
    err = errors.wrap(err)
    return user, err
end

local function get_users_by_ids(user_ids)
    local res = {}
    for i, user_id in pairs(user_ids) do
        local err
        res[i], err = get_user_by_id(user_id)
        if err ~= nil then
            return nil, err
        end
    end
    return res
end

M.get_user_by_id = get_user_by_id

function M.add_user(id, data)
    local bucket_id = M.vshard_router.bucket_id_strcrc32(id)

    local user_data = data
    user_data.id = id
    user_data.bucket_id = bucket_id

    local _, err = M.vshard_router.callrw(bucket_id, 'storage_api.add_user', {user_data}, {timeout = M.vshard_timeout})
    err = errors.wrap(err)
    if err ~= nil then
        return false, err
    end

    if user_data.phone_number ~= nil then
        M.identifiers_queue.add_identifier(user_data.id, 'phone_number', user_data.phone_number)
        log.info(("[add_user] add_identifier,name=phone_number,user_id=%s"):format(user_data.id))
        M.identifiers_queue.add_identifier(user_data.id, 'name_birthdate', {name=user_data.name, birthdate=user_data.birthdate})
        log.info(("[add_user] add_identifier,name=name_birthdate,user_id=%s"):format(user_data.id))
    end

    return true
end

function M.find_users_by_name(name)
    local storage_uris = cartridge_rpc.get_candidates('app.roles.storage', {leader_only = true})
    local res_by_uri, err = cartridge_pool.map_call('storage_api.get_users_by_name', {name}, {uri_list = storage_uris, timeout = M.vshard_timeout})

    if err ~= nil then
        return nil, err
    end

    local result = {}
    for _, res in pairs(res_by_uri) do
        for _, user in pairs(res) do
            table.insert(result, user)
        end
    end
    return result
end

function M.find_users_by_phone_number(phone_number)
    local user_ids, err = M.search_index.user_id.get_by_phone_number(phone_number)
    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end

    return get_users_by_ids(user_ids)
end

function M.find_users_by_name_birthdate(name, birthdate)
    local user_ids, err = M.search_index.user_id.get_by_name_birthdate(name, birthdate)
    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end

    return get_users_by_ids(user_ids)
end

function M.change_phone_number(id, new_phone_number)
    local bucket_id = M.vshard_router.bucket_id_strcrc32(id)
    local old_phone_number, err = M.vshard_router.callrw(bucket_id, 'storage_api.change_phone_number', {id, new_phone_number}, {timeout = M.vshard_timeout})
    err = errors.wrap(err)
    if err ~= nil then
        return false, err
    end

    M.identifiers_queue.delete_identifier(id, 'phone_number', old_phone_number)
    M.identifiers_queue.add_identifier(id, 'phone_number', new_phone_number)

    return true, err
end

return M
