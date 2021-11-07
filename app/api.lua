local M = {}

M.vshard_router = require('vshard.router')
M.vshard_timeout = 1

local errors = require('errors')
local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')

function M.get_user_by_id(id)
    local bucket_id = M.vshard_router.bucket_id_strcrc32(id)
    local user, err = M.vshard_router.callrw(bucket_id, 'storage_api.get_user_by_id', {id}, {timeout = M.vshard_timeout})
    err = errors.wrap(err)
    return user, err
end

function M.replace_user(id, data)
    local bucket_id = M.vshard_router.bucket_id_strcrc32(id)

    local user_data = data
    user_data.id = id
    user_data.bucket_id = bucket_id

    local _, err = M.vshard_router.callrw(bucket_id, 'storage_api.replace_user', {user_data}, {timeout = M.vshard_timeout})
    err = errors.wrap(err)
    if err ~= nil then
        return false, err
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

return M
