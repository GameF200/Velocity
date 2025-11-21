--!strict
--!optimize 2

--simple corutine pool module for +999 FPS, jk

local corutine_pool = {}
corutine_pool.__index = corutine_pool

function corutine_pool.new(max_size: number)
	local self = setmetatable({}, corutine_pool)
	self._max_size = max_size
	self._pool = {}
	self._active = {}
	self._queue = {}
	
	return self
end

function corutine_pool:create()
	return coroutine.create(function(task)
		while true do
			local Ok, Err = pcall(task.func, unpack(task.args or {}))
			if not Ok then
				print(`Corutine Error: {Err}`)
			end
			
			self:return_corutine(coroutine.running())
			
			task = coroutine.yield()
		end
	end)
end

function corutine_pool:get_corutine()
	if #self._pool > 0 then
		return table.remove(self._pool)
	elseif #self._active < self._max_size then
		return self:create()
	end
end

function corutine_pool:return_corutine(co: thread)
	if not self._active[co] then return end
	
	self._active[co] = nil
	table.insert(self._pool, co)
	
	if #self._queue > 0 then
		local task = table.remove(self._queue, 1)
		self:execute(task.func, unpack(task.args))
	end
end

function corutine_pool:execute(func, ...)
	local co = self:get_corutine()
	
	if co then
		self._active[co] = true
		local task = {func = func, args = {...}}
		coroutine.resume(co, task)
	else
		table.insert(self._queue, {func = func, args = {...}})
	end
end

function corutine_pool:wait_all()
	while next(self._active) do
		task.wait()
	end
end

return setmetatable(corutine_pool, {
	__call = function(_, max_size: number)
		return corutine_pool.new(max_size)
	end,
})