class YpJob extends AbstractJob{

  override var jobName: String = "job1"

  override def execute(): Unit = {
    println(jobName+" begin run")
  }
}
