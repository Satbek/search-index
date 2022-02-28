local log = require('log')

local M = {}

M.identifier = require('app.identifier')
M.vshard_timeout = 1
M.user_id = {}
M.vshard_router = require('vshard').router

local errors = require('errors')

function M.user_id.get_by_phone_number(phone_number)
    local identifier = M.identifier.phone_number(phone_number)

    local bucket_id = M.vshard_router.bucket_id_strcrc32(identifier.hash)

    local ids, err = M.vshard_router.callrw(bucket_id, 'search_storage_api.user_id.get_by_phone_number',
        {identifier.hash, identifier.data}, {timeout = M.vshard_timeout}
    )
    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end
    return ids
end

function M.user_id.add_phone_number_identifier(user_id, phone_number)
    local identifier = M.identifier.phone_number(phone_number)
    local bucket_id = M.vshard_router.bucket_id_strcrc32(identifier.hash)

    local ok, err = M.vshard_router.callrw(bucket_id, 'search_storage_api.user_id.add_phone_number_identifier',
        {user_id, identifier.hash, identifier.data, bucket_id}, {timeout = M.vshard_timeout}
    )
    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end
    return ok
end

function M.user_id.delete_phone_number_identifier(user_id, phone_number)
    local identifier = M.identifier.phone_number(phone_number)
    local bucket_id = M.vshard_router.bucket_id_strcrc32(identifier.hash)

    local ok, err = M.vshard_router.callrw(bucket_id, 'search_storage_api.user_id.delete_phone_number_identifier',
        {user_id, identifier.hash}, {timeout = M.vshard_timeout}
    )

    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end
    return ok
end

function M.user_id.get_by_name_birthdate(name, birthdate)
    local identifier = M.identifier.name_birthdate(name, birthdate)

    local bucket_id = M.vshard_router.bucket_id_strcrc32(identifier.hash)

    local ids, err = M.vshard_router.callrw(bucket_id, 'search_storage_api.user_id.get_by_name_birthdate',
        {identifier.hash, identifier.data}, {timeout = M.vshard_timeout}
    )
    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end
    return ids
end

function M.user_id.add_name_birthdate_identifier(user_id, name, birthdate)
    local identifier = M.identifier.name_birthdate(name, birthdate)
    local bucket_id = M.vshard_router.bucket_id_strcrc32(identifier.hash)

    local ok, err = M.vshard_router.callrw(bucket_id, 'search_storage_api.user_id.add_name_birthdate_identifier',
        {user_id, identifier.hash, identifier.data, bucket_id}, {timeout = M.vshard_timeout}
    )
    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end
    return ok
end

function M.user_id.delete_name_birthdate_identifier(user_id, name, birthdate)
    local identifier = M.identifier.name_birthdate(name, birthdate)
    local bucket_id = M.vshard_router.bucket_id_strcrc32(identifier.hash)

    local ok, err = M.vshard_router.callrw(bucket_id, 'search_storage_api.user_id.delete_name_birthdate_identifier',
        {user_id, identifier.hash}, {timeout = M.vshard_timeout}
    )

    if err ~= nil then
        err = errors.wrap(err)
        return nil, err
    end
    return ok
end


return M
