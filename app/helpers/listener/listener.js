"use strict";

const R = require("ramda");
const Q = require("q");

const Emitter = require("@wcm/module-helper").emitter;
const EventsModel = require("../../models/notification");

// FILTERS
const contentFilter = (item) => {
	const ct = R.pathOr(false, ["data", "contentType", "meta", "safeLabel"], item);

	if (!ct) {
		return false;
	}

	return (data) => {
		const ctLabel = R.pathOr(null, ["meta", "contentType", "meta", "safeLabel"], data);

		return ct === ctLabel;
	};
};

class Listener {
	get config() {
		return this._config;
	}
	get mappers() {
		return this._mappers.map((mapper) => ({ name: mapper.name }));
	}
	get emitters() {
		return this._emitters.map((emitter) => emitter.name === name);
	}

	constructor() {
		this._config = null;
		this._mappers = [];
		this._emitters = [];

		this.reinitialize();
	}

	// PUBLICS
	reinitialize() {
		this._reloadConfig();
		this._registerListeners();
	}

	removeListeners() {
		Emitter.offAny(this._selector);
	}

	registerMapper(mapperConfig) {
		this._mappers.push(mapperConfig);
	}

	unregisterMapper(name) {
		this._mappers = R.reject((mapper) => mapper.name === name)(this._mappers);
	}

	registerEmitter(emitterConfig) {
		this._emitters.push(emitterConfig);
	}

	unregisterEmitter(name) {
		this._emitters = R.reject((emitter) => emitter.name === name)(this._emitters);
	}


	// PRIVATES
	_reloadConfig() {
		EventsModel.find({})
			.populate("data.contentType")
			.lean()
			.then((response) => this._config = this._parseConfig(response));
	}

	_registerListeners() {
		Emitter.prependAny((name, data) => this._selector(name, data));
	}

	_getRequiredEvents(name, data) {
		if (this._config === null || !this._config[name]) {
			return;
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

	_selector(name, data) {
		const requiredEvents = this._getRequiredEvents(name, data);

		if (!Array.isArray(requiredEvents) || !requiredEvents.length) {
			return;
		}

		requiredEvents.forEach((event) => Q(this._sendEvent(event, data)));
	}


	_sendEvent(event, data) {
		if (!event || !event.topic || !data) {
			return Q.reject();
		}

		const topic = config.name + "_" + event.topic;

		console.log("SENDING EVENT", event, topic);
		console.log("DATA:", data);
	}

	_parseConfig(items) {
		const setFilter = (event, item) => {
			const source = R.pathOr(false, ["meta", "source"], item);

			if (source === "content") {
				return contentFilter(item);
			}
		};

		const reduceConfigItem = (acc, item) => R.compose(
			R.always(acc),
			R.forEach((event) => {
				if (!acc[event.name]) {
					acc[event.name] = [];
				}

				acc[event.name].push({
					topic: event.topic,
					filter: setFilter(event, item),
				});
			}),
			R.pathOr([], ["data", "events"])
		)(item);

		return R.reduce(reduceConfigItem, {})(items);
	};

}

module.exports = Listener;