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

        M.identifiers_queue.add_identifier(user_data.id, 'phone_number_hash', user_data.phone_number)
        log.info(("[add_user] add_identifier,name=phone_number_hash,user_id=%s"):format(user_data.id))
    end

    if user_data.email ~= nil then
        M.identifiers_queue.add_identifier(user_data.id, 'email', user_data.email)
        log.info(("[add_user] add_identifier,name=email,user_id=%s"):format(user_data.id))
    end

    if user_data.passport_num ~= nil then
        M.identifiers_queue.add_identifier(user_data.id, 'passport_num', user_data.passport_num)
        log.info(("[add_user] add_identifier,name=passport_num,user_id=%s"):format(user_data.id))
    end

    if user_data.metadata ~= nil and user_data.metadata.geo ~= nil then
        M.identifiers_queue.add_identifier(user_data.id, 'metadata_geo', user_data.metadata.geo)
        log.info(("[add_user] add_identifier,name=metadata_geo,user_id=%s"):format(user_data.id))
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

function M.find_users_by_email(email)
    local user_ids, err = M.search_index.user_id.get_by_email(email)
    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end

    return get_users_by_ids(user_ids)
end

function M.find_users_by_passport_num(passport_num)
    local user_ids, err = M.search_index.user_id.get_by_passport_num(passport_num)
    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end

    return get_users_by_ids(user_ids)
end

function M.find_users_by_geoposition(geoposition)
    local user_ids, err = M.search_index.user_id.get_by_geoposition(geoposition)
    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end

    return get_users_by_ids(user_ids)
end

function M.find_by_phone_number_hash(phone_number_hash)
    local user_ids, err = M.search_index.user_id.get_by_phone_number_hash(phone_number_hash)
    if err ~= nil then
        err  = errors.wrap(err)
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
    M.identifiers_queue.delete_identifier(id, 'phone_number_hash', old_phone_number)

    M.identifiers_queue.add_identifier(id, 'phone_number', new_phone_number)
    M.identifiers_queue.add_identifier(id, 'phone_number_hash', new_phone_number)

    return true, err
end

return M
