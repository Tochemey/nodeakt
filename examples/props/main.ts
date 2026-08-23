/*
 * MIT License
 *
 * Copyright (c) 2026 GoAkt Team
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/**
 * Showcase: construction is data.
 *
 * `Props.create(Greeter, "fr")` captures the class and its arguments so
 * the runtime can build the actor wherever it places it. A live instance
 * (`new Greeter("en")`) always stays on this core. `NODEAKT_PARALLELISM=1`
 * keeps this run local; the spawn call is the same as in `multicore`.
 *
 * Run: make props
 */

import { ActorSystem, Props } from "../../src/index";
import { Greeter } from "./greeter.actor";

// The system sizes itself to the machine at start; the environment
// variable is the one operational override, and 1 keeps every actor on
// this isolate.
process.env.NODEAKT_PARALLELISM = "1";

const system = new ActorSystem("props");
await system.start();

// Props form: the runtime builds the Greeter from the class + arguments.
const french = await system.spawn("french", Props.create(Greeter, "fr"));
const spanish = await system.spawn("spanish", Props.create(Greeter, "es"));

const outside = system.noSender();
console.log(await outside.ask(french, "Ada", 1_000));
console.log(await outside.ask(spanish, "Alan", 1_000));

// Instance form still works and always stays on the calling core.
const english = await system.spawn("english", new Greeter("en"));
console.log(await outside.ask(english, "Grace", 1_000));

await system.stop();
