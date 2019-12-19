const { pathOr, compose, always, forEach, reduce, __, or, invoker } = require("ramda");
const Q = require("q");
const request = require("request")

const Emitter = require("@wcm/module-helper").emitter; // WCM emitter
const moduleConfig = require("../variables");
const EventsModel = require("../../models/notification");

const toStringOrFalse = (input) => compose(
	or(__, false),
	invoker(0, "toString")
  )(input || "");

// FILTERS
const contentFilter = (item) => {
	const itemCTuuid = compose(
		toStringOrFalse,
		pathOr(false, ["data", "contentType", "_id"])
	)(item);

	if (!itemCTuuid) {
		return false;
	}

	return (data) => {
		const dataCTuuid = compose(
			toStringOrFalse,
			pathOr(__, ["meta", "contentType", "_id"], data),
			pathOr(false, ["meta", "contentType"])
		)(data);

		return itemCTuuid === dataCTuuid;
	};
};

const sendDefaultData = (event, data) => {
	const _conf = moduleConfig.get();

	if (!_conf.serverUrl) {
		return;
	}

	const serverUrl = _conf.serverUrl.replace(/\$\{topic\}/, event.topic);
	const method = _conf.method;

	request({
		url: serverUrl,
		method: method,
		body: data
	}, (err, response, body) => {
		if (err) {
			return console.log(err);
		}

		console.log("Default request result: ", body);
	});
}

class Listener {
	/**
	 * Config schema
	 * {
	 * 		[event_type]: {
	 * 			[event_method]: {
	 * 				topic: "String",
	 * 				mapper: "String", // optional
	 * 				emitter: "String", // optional
	 * 				filter: "Function" // optional
	 * 			}
	 * 		}
	 * }
	 */
	get config() {
		return this._config;
	}

	/**
	 * Mapper schema
	 * {
	 * 		[name]: "Function"
	 * }
	 */
	get mappers() {
		return Object.keys(this._mappers);
	}

	/**
	 * {
	 * 		[name]: "Function"
	 * }
	 */
	get emitters() {
		return Object.keys(this._emitters);
	}

	constructor() {
		this._config = null;
		this._mappers = {
			default: (eventName, event, data) => data
		};
		this._emitters = {
			default: (eventName, event, data) => sendDefaultData(event, data)
		};

		this.reinitialize();
	}

	// PUBLICS

	reinitialize() {
		this.reloadConfig();
		this.removeListeners();
		this._registerListeners();
	}

	// Stop listening for WCM events
	removeListeners() {
		if (!Array.isArray(Emitter.listenersAny()) || !Emitter.listenersAny().length) {
			return;
		}

		Emitter.offAny(this._selector);
	}

	registerMapper(name, fn) {
		this._mappers[name] = fn;
	}

	unregisterMapper(name) {
		delete this._mappers[name];
	}

	registerEmitter(name, fn) {
		this._emitters[name] = fn;
	}

	unregisterEmitter(name) {
		delete this._emitters[name];
	}

	reloadConfig() {
		EventsModel.find({})
			.populate("data.contentType")
			.lean()
			.exec()
			.then((response) => this._config = this._parseConfig(response))
	}

	// PRIVATES

	// Listen for all events of the module system
	_registerListeners() {
		Emitter.prependAny(this._selector.bind(this));
	}

	// Handle event
	_selector(name, data) {
		// Convert event name when event is an array
		const eventName = Array.isArray(name) ? name.join(".") : name;
		// Get configured events
		const requiredEvents = this._getRequiredEvents(name, data);

		// Don't do anything with this event since nothing is configured for it.
		if (!Array.isArray(requiredEvents) || !requiredEvents.length) {
			return;
		}

		// Send registerd events to the mappers and emitters (todo)
		requiredEvents.forEach((event) => Q(this._sendEvent(eventName, event, data)));
	}

	// Get configured events based on the name and data;
	_getRequiredEvents(name, data) {
		if (this._config === null || !this._config[name]) {
			return [];
		}

		const eventGroup = this._config[name];

		return eventGroup.filter((event) => {
			if (typeof event.filter === "function") {
				return event.filter(data);
			} else if (event.filter === false) {
				return false;
			}

			return true;
		});
	}

	_sendEvent(eventName, event, data) {
		if (!event || !event.topic || !data) {
			return Q.reject();
		}

		if (event.mapper && typeof this._mappers[event.mapper] === "function") {
			data = this._mappers[event.mapper](eventName, event, data);
		}

		if (this._emitters && typeof this._emitters[event.emitter] === "function") {
			return this._emitters[event.emitter](eventName, event, data);
		}

		// Default emitter
		return sendDefaultData(event, data);
	}

	// Setup config (cache) based on configured events
	_parseConfig(items) {
		const setFilter = (event, item) => {
			const source = pathOr(false, ["meta", "source"], item);

			if (source === "content") {
				return contentFilter(item);
			}
		};

		const reduceConfigItem = (acc, item) => compose(
			always(acc),
			forEach((event) => {
				const eventName = item.meta.source + "." + event.name;

				if (!acc[eventName]) {
					acc[eventName] = [];
				}

				acc[eventName].push({
					topic: event.topic,
					mapper: event.mapper,
					emitter: event.emitter,
					filter: setFilter(event, item),
				});
			}),
			pathOr([], ["data", "events"])
		)(item);

		return reduce(reduceConfigItem, {})(items);
	};

}

module.exports = Listener;
