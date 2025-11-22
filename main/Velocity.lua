--!optimize 2
--!nonstrict
--[[
	$$\    $$\           $$\                     $$\   $$\               
	$$ |   $$ |          $$ |                    \__|  $$ |              
	$$ |   $$ | $$$$$$\  $$ | $$$$$$\   $$$$$$$\ $$\ $$$$$$\   $$\   $$\ 
	\$$\  $$  |$$  __$$\ $$ |$$  __$$\ $$  _____|$$ |\_$$  _|  $$ |  $$ |
	 \$$\$$  / $$$$$$$$ |$$ |$$ /  $$ |$$ /      $$ |  $$ |    $$ |  $$ |
	  \$$$  /  $$   ____|$$ |$$ |  $$ |$$ |      $$ |  $$ |$$\ $$ |  $$ |
	   \$  /   \$$$$$$$\ $$ |\$$$$$$  |\$$$$$$$\ $$ |  \$$$$  |\$$$$$$$ |
	    \_/     \_______|\__| \______/  \_______|\__|   \____/  \____$$ |
	                                                           $$\   $$ |
	                                                           \$$$$$$  |
	                                                            \______/ 
	??????
	
	Velocity is a EXTREMELY fast module for data saving!
	
	@author super_sonic
	

	@license MIT
	@version 1.0.0
	@since 2025
	@changelog
		0.95.0:
		- Beta test
		- Changed Base64 to Base85
		- improved documentation
]]

--[[
	[Velocity]
		[Functions]
			- RegisterSessionAsync(player: Player, template: {[string]: any}, Optional<key: string>) <- register player session
			- RemovePlayerSession(player: Player) 													 <- removes player session
			- GetSession(player: Player) 															 <- gets player session
			- SaveAllSessions() 																	 <- saves all sessions to data store
			- LockSession(Session: VelocitySession) 												 <- locks player session
			- UnlockSession(Session: VelocitySession) 												 <- unlocks player session 
			
			[Session]
				- ReadKey(key: string) -> any 				<- read key from session
				- Read() -> {[string]: any}				    <- read all data from session
				- WriteKey(key: string, value: any) 		<- write key to session
				- Lock() -> boolean							<- locks player session
				- Unlock() 									<- unlocks player session
			
]]

local BinBuffer = require(script.Buffer)
local corutine_pool = require(script.corutine_pool).new(8)

-- idk why i add this ??
local ErrorTypes = {Session = "Session Conflict", DataStore =  "DataStore Error", MemoryStore = "Memory Store Error", Internal = "Internal Error"}

local DataStoreService = game:GetService("DataStoreService")
local MemoryStoreService = game:GetService("MemoryStoreService")

local SessionLocks: MemoryStoreSortedMap
do
	local Ok, Err = pcall(function()
		SessionLocks = MemoryStoreService:GetSortedMap("SessionLocks")
	end)
	if not Ok then
		warn(`[Velocity] MemoryStore Error: {Err}`)
	end
end

local HttpService = game:GetService("HttpService")
local Players = game:GetService("Players")
local RunService = game:GetService("RunService")

local Velocity = {}
Velocity.__index = Velocity

Velocity.DataStoreType = {
	Global = 1,
	Basic = 0
}

-- current sessions
local ActiveVelocityInstances = {}

-- memory store fallback system
local MEMORY_STORE_FALLBACK_MODE = false
local MEMORY_STORE_FALLBACK_LAST_RETRY_TIME = 0
local LocalLocks = {} -- <- fallback for memory store

local function is_memory_store_avaliable(): boolean
	if MEMORY_STORE_FALLBACK_MODE then
		if os.time() - MEMORY_STORE_FALLBACK_LAST_RETRY_TIME > 30 then
			MEMORY_STORE_FALLBACK_LAST_RETRY_TIME = os.time()
			local Ok = pcall(function()
				SessionLocks:GetAsync("health_check")
			end)
			if Ok then
				MEMORY_STORE_FALLBACK_MODE = false
			end
		else
			return false
		end
	end
	return true
end

-- lock managment
local function get_lock_key(userId: number): string
	return `velocity_lock_{userId}`
end

local LOCK_CONFIG = {
	TIMEOUT = 30,
	RETRY_DELAY = 0.1,
	MAX_RETRIES = 10,
	HEARTBEAT_INTERVAL = 10,
}

local function acquire_lock(userId: number, session_uuid: string): boolean
	local lockKey = get_lock_key(userId)

	if is_memory_store_avaliable() then
		for attempt = 1, LOCK_CONFIG.MAX_RETRIES do
			local success, result = pcall(function()
				return SessionLocks:SetAsync(
					lockKey,
					{
						server_id = game.JobId,
						timestamp = os.time(),
						session_uuid = session_uuid
					},
					LOCK_CONFIG.TIMEOUT
				)
			end)

			if success then
				return true
			else
				if attempt == LOCK_CONFIG.MAX_RETRIES then
					warn(`[Velocity] MemoryStore unavaliable, changing to fallback mode: {result}`)
					MEMORY_STORE_FALLBACK_MODE = true
					MEMORY_STORE_FALLBACK_LAST_RETRY_TIME = os.time()
					break
				end
				task.wait(LOCK_CONFIG.RETRY_DELAY)
			end
		end
	end

	if MEMORY_STORE_FALLBACK_MODE then
		local currentLock = LocalLocks[lockKey]

		if currentLock and os.time() - currentLock.timestamp < LOCK_CONFIG.TIMEOUT then
			return false
		end

		LocalLocks[lockKey] = {
			server_id = game.JobId,
			timestamp = os.time(),
			session_uuid = session_uuid
		}
		return true
	end

	return false
end

local function release_lock(userId: number)
	local lock_key = get_lock_key(userId)

	if is_memory_store_avaliable() then
		local Ok, Err = pcall(function()
			SessionLocks:RemoveAsync(lock_key)
		end)
		if not Ok then
			warn(`[Velocity] MemoryStore Error: {Err}`)
			LocalLocks[lock_key] = nil
		end
	else
		LocalLocks[lock_key] = nil
	end
end

local function verify_local_lock(lock_key: string, session_uuid: string): boolean
	local lock_data = LocalLocks[lock_key]
	if not lock_data then
		return false
	end

	if os.time() - lock_data.timestamp > LOCK_CONFIG.TIMEOUT then
		LocalLocks[lock_key] = nil
		return false
	end

	return if RunService:IsStudio() then true else (lock_data.server_id == game.JobId and lock_data.session_uuid == session_uuid)
end

local function verify_lock_ownership(userId: number, session_uuid: string): boolean
	local lock_key = get_lock_key(userId)

	if is_memory_store_avaliable() then
		local success, lock_data = pcall(function()
			return SessionLocks:GetAsync(lock_key)
		end)

		if success and lock_data then
			return if RunService:IsStudio() then true else (lock_data.server_id == game.JobId and lock_data.session_uuid == session_uuid)
		else
			return verify_local_lock(lock_key, session_uuid)
		end
	else
		return verify_local_lock(lock_key, session_uuid)
	end
end

local function cleanup_expired_locks()
	if MEMORY_STORE_FALLBACK_MODE then
		local currentTime = os.time()
		local expiredLocks = {}

		for lockKey, lockData in pairs(LocalLocks) do
			if currentTime - lockData.timestamp > LOCK_CONFIG.TIMEOUT then
				table.insert(expiredLocks, lockKey)
			end
		end

		for _, lockKey in ipairs(expiredLocks) do
			LocalLocks[lockKey] = nil
		end
	end
end

corutine_pool:execute(function()
	while true do
		task.wait(60)
		cleanup_expired_locks()
	end
end)

local function update_local_lock(lock_key: string, session_uuid: string)
	LocalLocks[lock_key] = {
		server_id = game.JobId,
		timestamp = os.time(),
		session_uuid = session_uuid
	}
end

local function start_lock_heartbeat(velocityInstance, userId: number, session_uuid: string)
	if velocityInstance.lock_heartbeat then return end

	velocityInstance.lock_heartbeat = true

	corutine_pool:execute(function()
		while velocityInstance.lock_heartbeat and velocityInstance.is_active and velocityInstance.is_locked do
			task.wait(LOCK_CONFIG.HEARTBEAT_INTERVAL)

			if not verify_lock_ownership(userId, session_uuid) then
				warn(`[Velocity] Lock ownership lost for user {userId}`)
				velocityInstance.is_locked = false
				break
			end

			local lock_key = get_lock_key(userId)

			if is_memory_store_avaliable() then
				local Ok, Err = pcall(function()
					SessionLocks:SetAsync(
						lock_key,
						{
							serverId = game.JobId,
							timestamp = os.time(),
							sessionId = session_uuid
						},
						LOCK_CONFIG.TIMEOUT
					)
				end)
				if not Ok then
					warn(`[Velocity] MemoryStore Error: {Err}`)
					update_local_lock(lock_key, session_uuid)
				end
			else
				update_local_lock(lock_key, session_uuid)
			end

			velocityInstance.last_lock_time = os.time()
		end

		velocityInstance.lock_heartbeat = false
	end)
end

-- session managment

-- blocks main thread while session loading
local function wait_for_session(velocityInstance, player: Player, timeout: number?)
	local startTime = os.clock()
	timeout = timeout or 5

	local session = velocityInstance.Sessions[player.UserId]

	if not session then
		return nil
	end

	while session and not session.is_active do
		if os.clock() - startTime > timeout then
			warn(`[Velocity] Timeout waiting for session for player {player.Name}`)
			velocityInstance:RemovePlayerSession(player)
			return nil
		end
		task.wait(0.01)
	end

	return session
end

function Velocity.New(data_store_name: string, data_store_type: number, template: {[string]: any})
	if typeof(data_store_name) ~= "string" or data_store_name:len() == 0 then
		error(`[Velocity] DataStore name must be a string`)
	end

	local dataStore = if data_store_type == 0 then DataStoreService:GetDataStore(data_store_name) 
		elseif data_store_type == 1 then DataStoreService:GetGlobalDataStore(data_store_name) 
		else DataStoreService:GetDataStore(data_store_name)

	local self = setmetatable({
		_template = template,
		_data_store = dataStore,
		Sessions = {},
		_data_store_name = data_store_name
	}, Velocity)


	ActiveVelocityInstances[data_store_name] = self

	return self
end

function Velocity:RegisterSessionAsync(player: Player, key: string)
	if not player or not player:IsDescendantOf(Players) then
		warn(`[Velocity] Player invalid, got {player}`)
	end
	
	local player_session = self.Sessions[player.UserId]

	if not player_session or player_session.is_active == false then
		local session_uuid = HttpService:GenerateGUID(false)

		player_session = {
			session_uuid = session_uuid,
			is_locked = false,
			data_buffer = BinBuffer.create({
				size = BinBuffer.bytes(24),
			}),
			is_active = false,
			key = if key then key else `Player_{player.UserId}`,
			lock_heartbeat = false,
			last_lock_time = 0,
			listeners = {},
			_template = self._template, 
			_data_store = self._data_store, 
			_player = player
		}
		self.Sessions[player.UserId] = player_session

		function player_session:Read(): {}
			if not self.is_active then
				warn(`[Velocity] Player {player.Name} session is not active`)
				return {}	
			elseif not self.is_locked then
				warn(`[Velocity] Reading from unlocked session for player {player.Name} is unsafe`)
				return {}
			else
				return BinBuffer.Read(self.data_buffer)
			end
		end

		function player_session:ReadKey(key: string): any
			if not self.is_active then
				warn(`[Velocity] cant read key in non active session`)
				return 
			elseif not self.is_locked then
				warn(`[Velocity] Reading from unlocked session for player {player.Name} is unsafe`)
				return
			else
				local data = BinBuffer.Read(self.data_buffer)

				for _key, value in pairs(data) do
					if _key == key then
						return value
					else
						continue
					end
				end
				return 
			end
		end

		function player_session:WriteKey(key: string, value: any): boolean
			if not self.is_active then
				warn(`[Velocity] cant write key in non active session`)
				return false
			elseif not self.is_locked then
				error(`[Velocity] Writing to unlocked session for player {player.Name} is unsafe`)
			else
				local data = BinBuffer.Read(self.data_buffer)
				local oldValue = data[key]

				if oldValue == value then
					return true
				end

				local new_buf = BinBuffer.create({size = self.data_buffer._writeOffset})
				local success = true

				for _key, val in pairs(data) do
					if _key == key then
						if not new_buf:Add(_key, value) then
							success = false
							break
						end
					else
						if not new_buf:Add(_key, val) then
							success = false
							break
						end
					end
				end

				if success then
					self.data_buffer:Destroy()
					self.data_buffer = new_buf

					if #self.listeners > 0 then
						for i = #self.listeners, 1, -1 do
							local listener = self.listeners[i]
							if not listener.disconnected then
								local listenerSuccess, listenerError = pcall(function()
									listener.callback(key, oldValue, value)
								end)
								if not listenerSuccess then
									warn("[Velocity] Listener Error:", listenerError)
								end
							else
								table.remove(self.listeners, i)
							end
						end
					end
					return true
				else
					new_buf:Destroy()
					return false
				end
			end
		end

		function player_session:ListenToKey(key: string, callback: (oldValue: any, newValue: any) -> ()): RBXScriptConnection
			return self:ListenToUpdate(function(updatedKey, oldValue, newValue)
				if updatedKey == key then
					callback(oldValue, newValue)
				end
			end)
		end

		function player_session:Lock(): boolean
			if self.is_locked then
				return true
			else
				if acquire_lock(player.UserId, self.session_uuid) then
					self.is_locked = true
					self.last_lock_time = os.time()
					start_lock_heartbeat(self, player.UserId, self.session_uuid)
					return true
				end
			end
			return false
		end

		function player_session:Reconcile()
			if not self.is_active then
				warn(`[Velocity] Cannot reconcile inactive session for player {player.Name}`)
				return
			end

			if not self.is_locked then
				warn(`[Velocity] Session must be locked for reconciliation`)
				return
			end

			local current_data = BinBuffer.Read(self.data_buffer)

			local missing_fields: {[string]: any} = {}

			for key, default_value in pairs(self._template) do
				if current_data[key] == nil then
					missing_fields[key] = default_value
				end
			end

			if next(missing_fields) ~= nil then
				local estimated_size = self.data_buffer._writeOffset + 1000 
				local new_buf = BinBuffer.create({size = estimated_size})

				for key, value in pairs(current_data) do
					if not new_buf:Add(key, value) then
						warn(`[Velocity] Failed to add existing key {key} during reconciliation`)
					end
				end

				for key, value in pairs(missing_fields) do
					if not new_buf:Add(key, value) then
						warn(`[Velocity] Failed to add missing key {key} during reconciliation`)
					end
				end

				self.data_buffer:Destroy()
				self.data_buffer = new_buf
			end
		end

		function player_session:Unlock()
			if not self.is_locked then
				return
			end

			self.lock_heartbeat = false
			self.is_locked = false
			release_lock(player.UserId)
		end

		function player_session:ListenToUpdate(callback: (key: string, oldValue: any, newValue: any) -> ()): RBXScriptConnection
			local listenerId = HttpService:GenerateGUID(false)

			local connection = {
				id = listenerId,
				callback = callback,
				disconnected = false,

				Disconnect = function(self)
					self.disconnected = true
					for i, listener in ipairs(player_session.listeners) do
						if listener.id == self.id then
							table.remove(player_session.listeners, i)
							break
						end
					end
				end
			}

			table.insert(player_session.listeners, connection)
			return connection
		end

		function player_session:IsLocked(): boolean
			return verify_lock_ownership(player.UserId, self.session_uuid)
		end

		corutine_pool:execute(function()
			local Ok, Err = pcall(function()
				local data: {[string]: any} = player_session._data_store:GetAsync(player_session.key)

				if data == nil then
					player_session.data_buffer:Clear()

					for key, value in pairs(player_session._template) do
						player_session.data_buffer:Add(key, value)
					end

					player_session._data_store:SetAsync(
						player_session.key,
						{data = player_session.data_buffer:Tostring()},
						{player.UserId}
					)
				else
					local buf = BinBuffer.Fromstring(data["data"]) :: BinBuffer.Buffer
					player_session.data_buffer = buf
				end

				player_session.is_active = true
			end)
			if not Ok then
				player_session:Unlock()
				local velocityInstance = self
				velocityInstance:RemovePlayerSession(player)
			end
		end)

		self.Sessions[player.UserId] = player_session
		return wait_for_session(self, player, 5)
	else
		warn(`[Velocity] Player {player.Name} already has a session`)
		return player_session
	end
end

function Velocity:LockSession(Session): boolean
	if not Session then
		warn(`[Velocity] Session not founded!`)
		return false
	else
		return Session:Lock()
	end
end

function Velocity:UnlockSession(Session)
	if not Session then
		warn(`[Velocity] Session not founded!`)
	else
		Session:Unlock()		
	end
end

function Velocity:RemovePlayerSession(player: Player)
	local Session = self.Sessions[player.UserId]
	if Session then
		local Ok, Err = pcall(function()
			if Session.is_locked then
				self._data_store:SetAsync(
					Session.key,
					{data = Session.data_buffer:Tostring()},
					{player.UserId}
				)
			end
		end)
		if not Ok then
			warn(`[Velocity] DataStore Error: {Err}`)
		end

		Session:Unlock()
		Session.data_buffer:Destroy()
		Session.is_active = false
		self.Sessions[player.UserId] = nil
	else
		warn(`[Velocity] Session for player {player.Name} not founded!`)
	end
end

function Velocity:IsSessionLocked(player: Player): boolean
	local session = self.Sessions[player.UserId]
	return session and session:IsLocked() or false
end

function Velocity:GetSession(player: Player)
	local player_session = self.Sessions[player.UserId]

	if player_session == nil or player_session.is_active == false then
		error(`[Velocity] Session for player {player.Name} not founded or not active!`)
	else
		return player_session
	end
end

function Velocity:SaveAllSessions()
	for user_id, session in pairs(self.Sessions) do
		local Ok, Err = pcall(function()
			self._data_store:SetAsync(session.key, 
				{data = session.data_buffer:Tostring()}, {user_id}
			)
		end)
		if not Ok then
			warn(`[Velocity] DataStore Error {Err}`)
		end
	end
end

if not game:GetService("RunService"):IsStudio() then
	game:BindToClose(function()
		for dataStoreName, velocityInstance in pairs(ActiveVelocityInstances) do
			for userId, session in pairs(velocityInstance.Sessions) do
				corutine_pool:execute(function()
					local player = Players:GetPlayerByUserId(userId)
					if player then
						velocityInstance:RemovePlayerSession(player)
					end
				end)
			end
		end
	end)
end

return Velocity