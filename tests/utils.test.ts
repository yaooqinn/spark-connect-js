import { PlanIdGenerator } from "../src/utils";
test("plan id generation", () => {
  const generator1 = PlanIdGenerator.getInstance()
  const generator2 = PlanIdGenerator.getInstance()
  expect(generator1).toBe(generator2)
  expect(generator1.getNextId()).toBe(0n)
  expect(generator1.getNextId()).toBe(1n)
  expect(generator2.getNextId()).toBe(2n)
  expect(generator2.getNextId()).toBe(3n)
  expect(generator1.getNextId()).toBe(4n)
});