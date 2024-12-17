import { PlanIdGenerator } from "../src/utils";
test("plan id generation", () => {
  const generator1 = PlanIdGenerator.getInstance()
  const generator2 = PlanIdGenerator.getInstance()
  expect(generator1).toBe(generator2)
  expect(generator1.getNextId()).toBe(0)
  expect(generator1.getNextId()).toBe(1)
  expect(generator2.getNextId()).toBe(2)
  expect(generator2.getNextId()).toBe(3)
  expect(generator1.getNextId()).toBe(4)
});