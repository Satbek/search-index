local M = {}

local log = require('log')
local json = require('json')

M.vshard_router = require('vshard.router')
M.vshard_timeout = 1
M.search_index = require('app.search_api')

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

    -- todo move to queue with layer recreation
    if user_data.phone_number ~= nil then
        local _, err_pn = M.search_index.user_id.add_phone_number_identifier(user_data.id, user_data.phone_number)
        if err_pn ~= nil then
            return false, err_pn
        end
        local _, err_pn_hash = M.search_index.user_id.add_phone_number_hash_identifier(user_data.id, user_data.phone_number)
        if err_pn ~= nil then
            return false, err_pn_hash
        end
    end

    if user_data.email ~= nil then
        local _, err_em = M.search_index.user_id.add_email_identifier(user_data.id, user_data.email)
        if err_em ~= nil then
            return false, err_em
        end
    end

    if user_data.passport_num ~= nil then
        local _, err_pn = M.search_index.user_id.add_passport_num_identifier(user_data.id, user_data.passport_num)
        if err_pn ~= nil then
            return false, err_pn
        end
    end

    if user_data.metadata ~= nil and user_data.metadata.geo ~= nil then
        local _, err_geo = M.search_index.user_id.add_geoposition_identifier(user_data.id, user_data.metadata.geo)
        if err_geo ~= nil then
            return false, err_geo
        end
    end

    return true, err
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

return M
