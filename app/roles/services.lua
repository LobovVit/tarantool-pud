local cartridge = require('cartridge')
local log = require('log')
local errors = require('errors')

local err_vshard_router = errors.new_class("Vshard routing error")

local function getDocByGuid(req)

    local guid = tonumber(req:stash('guid'))
    log.info('запросили getDocByGuid - %s', guid)
    local router = cartridge.service_get('vshard-router').get('hot')
    local bucket_id = router:bucket_id_strcrc32(guid)
    local document, error = err_vshard_router:pcall(
        router.call,
        router,
        bucket_id,
        'read',
        'getDocument',
        {guid}
    )

    if error then
        local resp = req:render({json = {
            info = "Internal error",
            error = error
        }})
        resp.status = 500
        return resp
    end

    if document == nil then
        local resp = req:render({json = { info = "Document not found" }})
        resp.status = 404
        return resp
    end

    document.bucket_id = nil
    local resp = req:render({json = document})
    resp.status = 200
    return resp

end

local function getDocByNum(req)
    local id = req:stash('num')
    return {
        status = 200,
        headers = { ['content-type'] = 'text/html; charset=utf8' },
        body = 'getDocByNum '..id
    }
end

local function createDoc(req)
    local document = req:json()
    log.info('запросили createDoc - %s', document)
    log.info('запросили createDoc2 - %s', document.guid)
    local router = cartridge.service_get('vshard-router').get('hot')
    local bucket_id = router:bucket_id_strcrc32(document.guid)
    log.info('!!!  bucket_id= %s', bucket_id)
    document.bucket_id = bucket_id
    local _, error = err_vshard_router:pcall(
        router.call,
        router,
        bucket_id,
        'write',
        'document_add',
        {document}
    )
    if error then
        local resp = req:render({json = {
            info = "Internal error",
            error = error
        }})
        resp.status = 500
        return resp
    end

    local resp = req:render({json = { info = "Successfully created" }})
    resp.status = 201
    return resp
end

local function init(opts) -- luacheck: no unused args
    -- if opts.is_master then
    -- end

    local httpd = assert(cartridge.service_get('httpd'), "Failed to get httpd serivce")
    httpd:route({method = 'GET', path = '/getDocByGuid/:guid'}, getDocByGuid)
    httpd:route({method = 'GET', path = '/getDocByNum/:num'}, getDocByNum)
    httpd:route({method = 'POST', path = '/createDoc'}, createDoc)

    return true
end

local function stop()
    return true
end

local function validate_config(conf_new, conf_old) -- luacheck: no unused args
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    -- if opts.is_master then
    -- end

    return true
end

return {
    role_name = 'app.roles.services',
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    dependencies = {'cartridge.roles.vshard-router'},
}
