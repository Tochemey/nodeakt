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

/** Test fixture: message classes that cross the isolate boundary. The
 * classes live in their own module so the actor that handles them and
 * the registration setup import the very same class objects. */

/** A unit of work sent to a placed actor. */
export class Job {
  constructor(id, note) {
    this.id = id;
    this.note = note;
  }
}

/** The answer a Job produces: carries whether the handler saw a real
 * Job instance, so tests can prove `instanceof` survived the hop. */
export class Receipt {
  constructor(id, sawJobInstance) {
    this.id = id;
    this.sawJobInstance = sawJobInstance;
  }

  stamped() {
    return `receipt-${this.id}`;
  }
}
