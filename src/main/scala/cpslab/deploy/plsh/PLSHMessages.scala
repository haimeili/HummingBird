package cpslab.deploy.plsh

trait PLSHMessage extends Serializable

case class WindowUpdate(lowerBound: Int, upperBound: Int) extends PLSHMessage

case class WindowUpdateNotification(id: Int) extends PLSHMessage

case class CapacityFullNotification(id: Int) extends PLSHMessage
