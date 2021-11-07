local t = require('luatest')
local g = t.group('integration_api')
local uuid = require('uuid')

local helper = require('test.helper')
local cluster = helper.cluster

g.before_all = function()
    g.cluster = helper.cluster
    g.cluster:start()
end

g.after_all = function()
    helper.stop_cluster(g.cluster)
end

g.test_sample = function()
    local server = cluster.main_server
    local response = server:http_request('post', '/admin/api', {json = {query = '{}'}})
    t.assert_equals(response.json, {data = {}})
    t.assert_equals(server.net_box:eval('return box.cfg.memtx_dir'), server.workdir)
end

g.test_metrics = function()
    local server = cluster.main_server
    local response = server:http_request('get', '/metrics')
    t.assert_equals(response.status, 200)
    t.assert_equals(response.reason, "Ok")
end

local function create_test_user()
    local id = uuid.str()
    local data = {
        name = 'test ' .. math.random(100000),
        id = id,
    }
    return data
end

g.test_get_user_by_id = function()
    local server = cluster.main_server
    local user = create_test_user()
    local _, err = server.net_box:call('api.replace_user', {user.id, user})
    t.assert_equals(err, nil)

    local expected_user, err = server.net_box:call('api.get_user_by_id', {user.id})
    t.assert_equals(err, nil)
    t.assert_equals(expected_user, user)
end
