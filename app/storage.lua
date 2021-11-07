local M = {}
local errors = require('errors')
local json = require('json')
local log = require('log')

M.user_not_exists_err = errors.new_class('user_not_exists')
M.replace_err = errors.new_class('replace_user')

local function user_tuple_to_output(t)
    local map = t:tomap({names_only = true})
    map.bucket_id = nil
    return map
end

function M.get_user_by_id(id)
    local user_t = box.space.user:get({id})
    if user_t == nil then
        return nil, M.user_not_exists_err:new('id=%s', id)
    end
    return user_tuple_to_output(user_t)
end


function M.replace_user(user_data)
    local user_t, err = box.space.user:frommap(user_data)
    if user_t == nil then
        return nil, M.replace_err:new('data=%s,err=%s', json.encode(user_data), err)
    end
    log.info("replace user: %s", user_data.id)
    box.space.user:replace(user_t)
end

return M
