package clustering.transformation

/**
  * ./gradlew clean run -PrunArgs="['backend', 'port']"
  */


object TransformationObject {

  def start(a: Seq[String]) = {
    val port = Seq(a(1))

    if(a(0) == "backend"){
      TransformationBackend.start(port)
    }
    if(a(0) == "frontend"){
      TransformationFrontend.start(port)
    }

  }
}
