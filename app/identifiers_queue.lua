local M = {}
local log = require('log')
local queue = require('queue')

function M.init()
    -- add a log record on task completion
    local function otc_cb(task, stats_data)
        if stats_data == 'delete' then
            log.info("task %s is done", task[1])
        end
    end

    queue.create_tube('identifiers_tube', 'fifo', {on_task_change = otc_cb})
    rawset(_G, 'queue', queue)
end

function M.add_identifier(user_id, identifier_name, data)
    queue.tube.identifiers_tube:put({
        user_id = user_id,
        identifier_name = identifier_name,
        data = data,
        operation = 'add',
    })
end

function M.delete_identifier(user_id, identifier_name, data)
    queue.tube.identifiers_tube:put({
        user_id = user_id,
        identifier_name = identifier_name,
        data = data,
        operation = 'delete',
    })
end

return M
