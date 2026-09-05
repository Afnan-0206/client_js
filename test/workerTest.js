// Copyright The Prometheus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

const { setTimeout: delay } = require('timers/promises');
const { BroadcastChannel } = require('worker_threads');
const Registry = require('../lib/worker');

const ACK = '@prometheus-io/client:ack';
const ANNOUNCEMENT = '@prometheus-io/client:announcement';
const GET_METRICS_REQ = '@prometheus-io/client:getMetricsReq';
const GET_METRICS_RES = '@prometheus-io/client:getMetricsRes';
const GOODBYE = '@prometheus-io/client:goodbye';
const WORKER_SCRAPE_FAILURES = 'prom_client_worker_scrape_failures';

function metric(value) {
	return {
		help: 'test metric',
		name: 'test_metric',
		type: 'gauge',
		values: [{ labels: {}, value }],
		aggregator: 'sum',
	};
}

function counter(value) {
	return {
		help: 'test metric two',
		name: 'test_metric_two_counter',
		type: 'counter',
		values: [{ labels: {}, value }],
		aggregator: 'sum',
	};
}

describe.each([
	['Prometheus', Registry.PROMETHEUS_CONTENT_TYPE],
	['OpenMetrics', Registry.OPENMETRICS_CONTENT_TYPE],
])('%s AggregatorRegistry', (tag, regType) => {
	beforeEach(() => {
		Registry.globalRegistry.setContentType(regType);
	});

	describe('WorkerRegistry.workerMetrics()', () => {
		let AggregatorRegistry;
		let announcementChannel;
		let registry;
		let discovery;

		beforeEach(async () => {
			jest.resetModules();
			AggregatorRegistry = require('../lib/worker');
			registry = new AggregatorRegistry(regType);
			announcementChannel = new BroadcastChannel(
				'@prometheus-io/client:announce',
			).unref();

			discovery = new Promise(resolve => {
				announcementChannel.addEventListener('message', async event => {
					if (event.data.type === ANNOUNCEMENT && !event.data.primary) {
						resolve(event);
					}
				});
			});
		});

		afterEach(async () => {
			announcementChannel.close();
		});

		it('works properly if there are no workers', async () => {
			const metrics = await registry.workerMetrics();
			expect(metrics).toContain(`${WORKER_SCRAPE_FAILURES}_count 0`);
		});

		it('formats in the correct content type', async () => {
			const threadId = 211;
			const name = `@prometheus-io/client:worker:${threadId}`;
			const channel = new BroadcastChannel(name).unref();

			announcementChannel.postMessage({
				type: ANNOUNCEMENT,
				name,
				threadId,
			});

			await discovery; // Let announcements arrive

			announcementChannel.addEventListener('message', async event => {
				if (event.data.type !== GET_METRICS_REQ) return;

				channel.postMessage({
					type: GET_METRICS_RES,
					requestId: event.data.requestId,
					threadId,
					metrics: [[counter(1.2345)]],
				});
			});

			try {
				const result = await registry.workerMetrics();
				if (regType === Registry.PROMETHEUS_CONTENT_TYPE) {
					expect(result).toContain('test_metric_two_counter 1.2345');
				} else {
					expect(result).toContain('test_metric_two_counter_total 1.2345');
				}
			} finally {
				channel.close();
			}
		});

		it('aggregates worker responses in thread id order', async () => {
			const responders = [1, 2, 3].map(threadId => {
				const name = `@prometheus-io/client:worker:${threadId}`;
				const channel = new BroadcastChannel(name).unref();

				announcementChannel.postMessage({
					type: ANNOUNCEMENT,
					name,
					threadId,
				});

				return { threadId, channel };
			});

			await discovery; // Let announcements arrive

			let finishSendingResponses;
			const responsesSent = new Promise(resolve => {
				finishSendingResponses = resolve;
			});
			announcementChannel.addEventListener('message', async event => {
				if (event.data.type !== GET_METRICS_REQ) return;

				for (const [threadId, value] of [
					[3, 0.3437699],
					[1, 0.5848208],
					[2, 0.5479198],
				]) {
					responders[threadId - 1].channel.postMessage({
						type: GET_METRICS_RES,
						requestId: event.data.requestId,
						threadId,
						metrics: [[metric(value)]],
					});
					await delay(5);
				}
				finishSendingResponses();
			});

			try {
				const result = await registry.workerMetrics();
				await responsesSent;
				expect(result).toContain('test_metric 1.4765105');
			} finally {
				for (const responder of responders) responder.channel.close();
			}
		});

		it('accumulate stats from terminated workers', async () => {
			jest.resetModules();
			const AggregatorRegistry = require('../lib/worker');
			const registry = new AggregatorRegistry(regType);
			const announcementChannel = new BroadcastChannel(
				'@prometheus-io/client:announce',
			).unref();

			const threadId = 134;
			const name = `@prometheus-io/client:worker:${threadId}`;
			const channel = new BroadcastChannel(name).unref();

			announcementChannel.postMessage({
				type: ANNOUNCEMENT,
				name,
				threadId,
			});

			await delay(5); // Let announcements arrive

			const ack = new Promise(resolve => {
				channel.addEventListener('message', async event => {
					if (event.data.type === ACK) {
						resolve(event);
					}
				});
			});

			channel.postMessage({
				type: GOODBYE,
				threadId,
				metrics: [[metric(0.123456)]],
			});

			await ack;

			try {
				const result = await registry.workerMetrics();
				expect(result).toContain('test_metric 0.123456');
			} finally {
				announcementChannel.close();
				channel.close();
			}
		});
	});

	describe('shutdown()', () => {
		let AggregatorRegistry;
		let announcementChannel;
		let registry;
		let discovery;

		beforeEach(async () => {
			jest.resetModules();
			AggregatorRegistry = require('../lib/worker');

			announcementChannel = new BroadcastChannel(
				'@prometheus-io/client:announce',
			).unref();

			registry = new AggregatorRegistry(regType);

			discovery = new Promise(resolve => {
				announcementChannel.addEventListener('message', async event => {
					if (event.data.type === ANNOUNCEMENT && !event.data.primary) {
						resolve(event);
					}
				});
			});
		});

		afterEach(() => {
			announcementChannel.close();
		});

		it('returns immediately on no outstanding requests', async () => {
			await expect(registry.shutdown()).resolves.not.toThrow();
		});

		it('sends data back to the primary', async () => {
			jest.resetModules();
			AggregatorRegistry = require('../lib/worker');

			const workerRegistry = new AggregatorRegistry(regType, false);
			const name = `@prometheus-io/client:worker:0`;
			const channel = new BroadcastChannel(name).unref();

			const { Gauge } = require('../index');
			const gauge = new Gauge({ name: 'primary_gauge_test', help: 'test' });

			gauge.set(0.8675309);

			// wait until worker has processed the ACk before continuing
			const metrics = new Promise(resolve => {
				channel.addEventListener('message', async event => {
					if (event.data.type === GOODBYE) {
						channel.postMessage({ type: ACK, requestId: 0, threadId: 0 });
						resolve(event.data.metrics);
					}
				});
			});

			try {
				await workerRegistry.shutdown();
				const expected = {
					aggregator: 'sum',
					help: 'test',
					name: 'primary_gauge_test',
					type: 'gauge',
					values: [
						{
							labels: {},
							value: 0.8675309,
						},
					],
				};

				const histogram = AggregatorRegistry.globalRegistry.getSingleMetric(
					WORKER_SCRAPE_FAILURES,
				);
				await expect(metrics).resolves.toEqual([
					[await histogram.get(), expected],
				]);
			} finally {
				channel.close();
			}
		});

		describe('with workers', () => {
			let channel;

			beforeEach(async () => {
				const threadId = 22;
				const name = `@prometheus-io/client:test-worker:${threadId}`;
				channel = new BroadcastChannel(name).unref();

				announcementChannel.postMessage({
					type: ANNOUNCEMENT,
					name,
					threadId,
				});

				announcementChannel.addEventListener('message', async event => {
					if (event.data.type === GET_METRICS_REQ) {
						channel.postMessage({
							type: GET_METRICS_RES,
							requestId: event.data.requestId,
							threadId: 22,
							metrics: [[metric(2)]],
						});
					}
				});

				await discovery;
			});

			afterEach(() => {
				channel.close();
			});

			it('waits for pending requests', async () => {
				const results = [];
				const promise = registry.workerMetrics().then(() => results.push(1));
				const shutdown = registry.shutdown().then(() => results.push(2));
				await Promise.all([promise, shutdown]);

				expect(results).toEqual([1, 2]);
			});
		});
	});

	describe('message handling', () => {
		it("listeners don't accumulate", () => {
			for (let i = 0; i < 30; i++) {
				jest.resetModules();

				const AggregatorRegistry = require('../lib/worker');
				const ar = new AggregatorRegistry(regType);
			}
		});

		it('does not error out on unexpected (or late) responses', () => {
			jest.resetModules();

			const WorkerRegistry = require('../lib/worker');
			const registry = new WorkerRegistry(regType);
			const announcementChannel = new BroadcastChannel(
				'@prometheus-io/client:announce',
			).unref();
			const threadId = 20;
			const name = `@prometheus-io/client:test-worker:${threadId}`;
			const channel = new BroadcastChannel(name).unref();

			announcementChannel.postMessage({
				type: ANNOUNCEMENT,
				name,
				threadId,
			});

			//Emulate a response that has been deleted from requests
			const unexpected = {
				type: '@prometheus-io/client:getMetricsRes',
				metrics: ['{}'],
				requestId: -3,
			};

			try {
				expect(() => channel.postMessage(unexpected)).not.toThrow();
			} finally {
				channel.close();
			}
		});
	});
});

describe.each([
	['Prometheus', Registry.PROMETHEUS_CONTENT_TYPE],
	['OpenMetrics', Registry.OPENMETRICS_CONTENT_TYPE],
])('%s worker scrape failures', (tag, regType) => {
	let WorkerRegistry;
	let registry;
	let channels;
	let announcementChannel;

	beforeEach(() => {
		jest.resetModules();
		jest.useFakeTimers();
		channels = [];

		// Deliver messages explicitly so timeout and late-response tests do not
		// depend on BroadcastChannel scheduling or wall-clock delays.
		jest.doMock('node:worker_threads', () => {
			return {
				isMainThread: true,
				threadId: 0,
				BroadcastChannel: class {
					constructor(name) {
						this.name = name;
						this.listeners = [];
						this.postMessage = jest.fn();
						channels.push(this);
					}

					unref() {
						return this;
					}

					close() {}

					addEventListener(type, listener) {
						if (type === 'message') this.listeners.push(listener);
					}

					receive(data) {
						return Promise.all(
							this.listeners.map(listener => listener({ data })),
						);
					}
				},
			};
		});

		WorkerRegistry = require('../lib/worker');
		registry = new WorkerRegistry(regType);
		announcementChannel = channels[0];
	});

	afterEach(() => {
		jest.useRealTimers();
		jest.restoreAllMocks();
		jest.dontMock('node:worker_threads');
	});

	async function announceWorker(threadId) {
		const name = `@prometheus-io/client:worker:${threadId}`;
		await announcementChannel.receive({ type: ANNOUNCEMENT, name, threadId });
		return channels.find(channel => channel.name === name);
	}

	function expectFailures(metrics, count, sum, zeros = 0) {
		expect(
			metrics
				.split('\n')
				.filter(line => line.startsWith(`${WORKER_SCRAPE_FAILURES}_count `)),
		).toEqual([`${WORKER_SCRAPE_FAILURES}_count ${count}`]);
		expect(metrics).toContain(`${WORKER_SCRAPE_FAILURES}_sum ${sum}\n`);
		expect(metrics).toContain(
			`${WORKER_SCRAPE_FAILURES}_bucket{le="0"} ${zeros}\n`,
		);
		expect(metrics.endsWith('# EOF\n')).toBe(
			regType === Registry.OPENMETRICS_CONTENT_TYPE,
		);
	}

	it('retains consecutive timeouts after the last worker leaves and ignores late errors', async () => {
		const workers = [];
		for (const id of [1, 2, 3]) workers.push(await announceWorker(id));

		for (const requestId of [0, 1]) {
			const rejection = expect(registry.workerMetrics()).rejects.toThrow(
				`Operation timed out. ${2 - requestId} outstanding responses.`,
			);
			for (let index = 0; index <= requestId; index++) {
				await workers[index].receive({
					type: GET_METRICS_RES,
					requestId,
					metrics: [[]],
				});
			}
			await jest.advanceTimersByTimeAsync(5_000);
			await rejection;

			await workers[2].receive({
				type: GET_METRICS_RES,
				requestId,
				error: 'late failure',
			});
		}

		for (const worker of workers) {
			await worker.receive({ type: GOODBYE, metrics: [[]] });
		}
		expect(WorkerRegistry.workerCount()).toBe(0);
		const recovered = await registry.workerMetrics();
		expectFailures(recovered, 2, 3);
		expect(recovered).toContain(`${WORKER_SCRAPE_FAILURES}_bucket{le="1"} 1\n`);
		expect(recovered).toContain(`${WORKER_SCRAPE_FAILURES}_bucket{le="2"} 2\n`);
		expectFailures(await registry.workerMetrics(), 2, 3);
	});

	it.each(['worker collection failed', 'Timeout'])(
		'records the worker error %s once without counting a waiting worker',
		async errorMessage => {
			const worker = await announceWorker(1);
			const waitingWorker = await announceWorker(2);
			const rejection = expect(registry.workerMetrics()).rejects.toThrow(
				new Error(errorMessage),
			);
			const error = {
				type: GET_METRICS_RES,
				requestId: 0,
				error: errorMessage,
			};
			await worker.receive(error);
			await rejection;
			await worker.receive(error);

			const recovered = registry.workerMetrics();
			await worker.receive({
				type: GET_METRICS_RES,
				requestId: 1,
				metrics: [[metric(7)]],
			});
			await waitingWorker.receive({
				type: GET_METRICS_RES,
				requestId: 1,
				metrics: [[]],
			});
			const metrics = await recovered;
			expectFailures(metrics, 1, 1);
			expect(metrics).toContain('test_metric 7\n');
		},
	);

	it.each([0, 1])(
		'records zero for a broadcast failure with %i workers and settles the request',
		async workerCount => {
			const worker = workerCount ? await announceWorker(1) : undefined;
			const error = new TypeError('postMessage failed');
			announcementChannel.postMessage.mockImplementationOnce(() => {
				throw error;
			});

			await expect(registry.workerMetrics()).rejects.toBe(error);
			await jest.advanceTimersByTimeAsync(0);
			expect(jest.getTimerCount()).toBe(0);
			await expect(registry.shutdown()).resolves.toBeUndefined();

			const recovered = registry.workerMetrics();
			if (worker) {
				await worker.receive({
					type: GET_METRICS_RES,
					requestId: 1,
					metrics: [[]],
				});
			}
			expectFailures(await recovered, 1, 0, 1);
		},
	);

	it.each(['default', 'custom'])(
		'reports %s registry collection errors to the parent',
		async registryType => {
			const MetricRegistry = require('../lib/registry');
			const source =
				registryType === 'default'
					? MetricRegistry.globalRegistry
					: new MetricRegistry();
			WorkerRegistry.setRegistries(source);
			jest
				.spyOn(source, 'getMetricsAsJSON')
				.mockRejectedValueOnce(new Error('worker collection failed'));
			const workerChannel = channels[1];

			await announcementChannel.receive({
				type: GET_METRICS_REQ,
				requestId: 42,
			});
			expect(workerChannel.postMessage).toHaveBeenLastCalledWith({
				type: GET_METRICS_RES,
				requestId: 42,
				error: 'worker collection failed',
			});

			await announcementChannel.receive({
				type: GET_METRICS_REQ,
				requestId: 43,
			});
			expect(workerChannel.postMessage).toHaveBeenLastCalledWith({
				type: GET_METRICS_RES,
				requestId: 43,
				threadId: 0,
				metrics: [await source.getMetricsAsJSON()],
			});
			expect(
				MetricRegistry.globalRegistry.getSingleMetric(WORKER_SCRAPE_FAILURES),
			).toBeDefined();
		},
	);

	it.each(['default', 'custom', 'custom with internal histogram'])(
		'exports a %s registry failure once and preserves a collector error named Timeout',
		async registryType => {
			const MetricRegistry = require('../lib/registry');
			const Gauge = require('../lib/gauge');
			const source =
				registryType === 'default'
					? MetricRegistry.globalRegistry
					: new MetricRegistry();
			WorkerRegistry.setRegistries(source);
			const histogram = MetricRegistry.globalRegistry.getSingleMetric(
				WORKER_SCRAPE_FAILURES,
			);
			expect(histogram).toBeDefined();
			if (registryType === 'custom with internal histogram')
				source.registerMetric(histogram);
			const gauge = new Gauge({
				name: 'coordinator_value',
				help: 'test',
				registers: [source],
			});
			gauge.set(17);
			const error = new TypeError('Timeout');
			jest.spyOn(source, 'getMetricsAsJSON').mockRejectedValueOnce(error);
			await expect(registry.workerMetrics()).rejects.toBe(error);

			for (let scrape = 0; scrape < 2; scrape++) {
				const metrics = await registry.workerMetrics();
				expectFailures(metrics, 1, 0, 1);
				expect(metrics).toContain('coordinator_value 17\n');
			}
			MetricRegistry.globalRegistry.resetMetrics();
			expect((await histogram.get()).values).toEqual([]);
			source.getMetricsAsJSON.mockRejectedValueOnce(error);
			await expect(registry.workerMetrics()).rejects.toBe(error);
			expectFailures(await registry.workerMetrics(), 1, 0, 1);
		},
	);
});
