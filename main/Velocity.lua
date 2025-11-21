--!optimize 2
--!strict
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


-- using 1 data store for all

local VELOCITY_DATA_STORE: DataStore
do
	local Ok, Err = pcall(function()
		VELOCITY_DATA_STORE = DataStoreService:GetDataStore("VELOCITY_DATA_STORE")
	end)
	if not Ok then
		warn(`[Velocity] DataStore Error: {Err}`)
	end
end

-- types
export type VelocitySession = {
	session_uuid: string,
	is_locked: boolean,
	data_buffer: BinBuffer.Buffer,
	is_active: boolean,
	key: string,
	last_lock_time: number,
	lock_heartbeat: boolean,
	listeners: {},
	

	ReadKey: (key: string) -> any,
	Read: () -> {[string]: any},
	WriteKey: (key: string) -> (),
	Lock: () -> (boolean),
	Unlock: () -> (),
	IsLocked: () -> (boolean),
	ListenToKey: (key: string, callback: (old_value: any, new_value: any) -> ()) -> (),
	ListenToUpdate: (callback: (key: string, old_value: any, new_value: any) -> ()) -> (),
	Reconcile: () -> ()
}

-- current sessions
local Sessions: {[number]: VelocitySession} = {
	--[player_uuid] = {
	-- [session_uuid] = "123456789",     								   <- current session uuid
	-- [is_locked] = true,               								   <- is locked by server
	-- [data_buffer] = BinBuffer object, 								   <- main data buffer 
	-- [is_active] = true,               								   <- is session active or not
	-- [is_locked] = true,               								   <- is locked by server
	-- [key] = "Player_123456789",       								   <- player key in data store
	-- [last_lock_time] = 0,             								   <- time when lock was last updated
	-- [lock_heartbeat] = false          								   <- enable auto locking
	-- [IsLocked, Read, Lock, Unlock, ReadKey, WriteKey, Reconcile]        <- functions
	--}
}

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

local function acquire_lock(userId: number): boolean
	local lockKey = get_lock_key(userId)
	local Session = Sessions[userId]
	if not Session then
		warn(`[Velocity] Session for player {userId} not found!`)
		return false	
	end

	for attempt = 1, LOCK_CONFIG.MAX_RETRIES do
		local Ok, Err = pcall(function()
			return SessionLocks:SetAsync(
				lockKey,
				{
					server_id = game.JobId,
					timestamp = os.time(),
					session_uuid = Session.session_uuid
				},
				LOCK_CONFIG.TIMEOUT
			)
		end)

		if Ok then
			return true
		else
			task.wait(LOCK_CONFIG.RETRY_DELAY)
		end
	end

	return false
end

local function release_lock(userId: number)
	local lock_key = get_lock_key(userId)
	local Ok, Err = pcall(function()
		SessionLocks:RemoveAsync(lock_key)
	end)
	if not Ok then
		warn(`[Velocity] MemoryStore Error: {Err}`)
	end
end


local function verify_lock_ownership(userId: number): boolean
	local lock_key = get_lock_key(userId)
	local Ok, lock_data = pcall(function()
		return SessionLocks:GetAsync(lock_key)
	end)


	if Ok and lock_data then
		return if RunService:IsStudio() then true else lock_data.server_id == game.JobId
	end

	return false
end

local function start_lock_heartbeat(Session: VelocitySession, userId: number)
	if Session.lock_heartbeat then return end

	Session.lock_heartbeat = true

	corutine_pool:execute(function()
		while Session.lock_heartbeat and Session.is_active and Session.is_locked do
			task.wait(LOCK_CONFIG.HEARTBEAT_INTERVAL)

			if not verify_lock_ownership(userId) then
				warn(`[Velocity] Lock ownership lost for user {userId}`)
				Session.is_locked = false
				break
			end

			local lock_key = get_lock_key(userId)
			local Ok, Err = pcall(function()
				SessionLocks:SetAsync(
					lock_key,
					{
						serverId = game.JobId,
						timestamp = os.time(),
						sessionId = Session.session_uuid
					},
					LOCK_CONFIG.TIMEOUT
				)
			end)
			if not Ok then
				warn(`[Velocity] MemoryStore Error: {Err}`)
			end

			Session.last_lock_time = os.time()
		end

		Session.lock_heartbeat = false
	end)
end

-- session managment
local function wait_for_session(player: Player, timeout: number?): VelocitySession?
	local startTime = os.clock()
	timeout = timeout or 5

	local session = Sessions[player.UserId]

	if not session then
		return nil
	end

	while session and not session.is_active do
		if os.clock() - startTime > timeout then
			warn(`[Velocity] Timeout waiting for session for player {player.Name}`)
			Velocity:RemovePlayerSession(player)
			return nil
		end
		task.wait(0.01)
	end

	return session
end

local function save_all_sessions()	
	for user_id, session in pairs(Sessions) do
		local Ok, Err = pcall(function()
			VELOCITY_DATA_STORE:SetAsync(session.key, 
				{data = session.data_buffer:Tostring()}, {user_id}
			)
		end)
		if not Ok then
			warn(`[Velocity] DataStore Error {Err}`)
		end
	end
end


function Velocity:RegisterSessionAsync(player: Player, template: {[string]: any}, key: string): VelocitySession
	if typeof(template) ~= "table" then
		error(`[Velocity] Template must be a table`)
	end
	if not player or not player:IsDescendantOf(Players) then
		warn(`[Velocity] Player invalid, got {player}`)
	end

	local player_session: VelocitySession = Sessions[player.UserId]
	
	if not player_session or player_session.is_active == false then
		player_session = {
			session_uuid = HttpService:GenerateGUID(false),
			is_locked = false,
			data_buffer = BinBuffer.create(
				{
					size = BinBuffer.bytes(24),
				}
			),
			is_active = false,
			key = if key then key else `Player_{player.UserId}`,
			lock_heartbeat = false,
			last_lock_time = 0,
			listeners = {}
		} :: VelocitySession
		Sessions[player.UserId] = player_session

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
				if acquire_lock(player.UserId) then
					self.is_locked = true
					self.last_lock_time = os.time()
					start_lock_heartbeat(self, player.UserId)
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
			for key, default_value in pairs(template) do
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
					for i, listener in ipairs(self.listeners) do
						if listener.id == self.id then
							table.remove(self.listeners, i)
							break
						end
					end
				end
			}

			table.insert(self.listeners, connection)
			return connection
		end


		function player_session:IsLocked(): boolean
			return verify_lock_ownership(player.UserId)
		end

		corutine_pool:execute(function()
			local success, error = pcall(function()

				local data: {[string]: any} = VELOCITY_DATA_STORE:GetAsync(player_session.key)

				if data == nil then
					player_session.data_buffer:Clear()

					for key, value in pairs(template) do
						player_session.data_buffer:Add(key, value)
					end

					VELOCITY_DATA_STORE:SetAsync(
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
			if not success then
				player_session:Unlock()
				Velocity:RemovePlayerSession(player)
			end
		end)

		Sessions[player.UserId] = player_session
		return wait_for_session(player, 5) :: VelocitySession
	else
		warn(`[Velocity] Player {player.Name} already has a session`)
		return player_session :: VelocitySession
	end
end

function Velocity:LockSession(Session: VelocitySession): boolean
	if not Session then
		warn(`[Velocity] Session not founded!`)
		return false
	else
		return Session:Lock()
	end
end

function Velocity:UnlockSession(Session: VelocitySession)
	if not Session then
		warn(`[Velocity] Session not founded!`)
	else
		Session:Unlock()		
	end
end

function Velocity:RemovePlayerSession(player: Player)
	local Session = Sessions[player.UserId]
	if Session then
		local Ok, Err = pcall(function()
			if Session.is_locked then
				VELOCITY_DATA_STORE:SetAsync(
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
		Sessions[player.UserId] = nil
	else
		warn(`[Velocity] Session for player {player.Name} not founded!`)
	end
end

function Velocity:IsSessionLocked(player: Player): boolean
	local session = Sessions[player.UserId]
	return session and session:IsLocked() or false
end

function Velocity:GetSession(player: Player): VelocitySession
	local player_session = Sessions[player.UserId]

	if player_session == nil or player_session.is_active == false then
		error(`[Velocity] Session for player {player.Name} not founded or not active!`)
	else
		return player_session :: VelocitySession
	end
end

function Velocity:SaveAllSessions()
	save_all_sessions()
end

game:BindToClose(function()
	for _index, player in pairs(Players:GetPlayers()) do
		corutine_pool:execute(function()
			Velocity:RemovePlayerSession(player)
		end)
	end
end)
return Velocity