local M = {}

M.vshard_router = require('vshard.router')
M.vshard_timeout = 1

local errors = require('errors')

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

return M
