package clustering.transformation

/**
  * Created by pzaytsev on 11/5/17.
  */
object TransformationMessages {

  final case class TransformationJob(text: String)
  final case class TransformationResult(text: String)
  final case class JobFailed(reason: String, job: TransformationJob)
  case object BackendRegistration
}
