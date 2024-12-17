export class PlanIdGenerator {
  private static instance: PlanIdGenerator;
  private currentId: number = 0;
  private constructor() {}

  public static getInstance(): PlanIdGenerator {
    if (!PlanIdGenerator.instance) {
      PlanIdGenerator.instance = new PlanIdGenerator();
    }
    return PlanIdGenerator.instance;
  }

  public getNextId(): number {
    return this.currentId++;
  }
}