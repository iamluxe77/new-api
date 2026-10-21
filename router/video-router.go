package router

import (
	"github.com/QuantumNous/new-api/controller"
	"github.com/QuantumNous/new-api/middleware"

	"github.com/gin-gonic/gin"
)

func SetVideoRouter(router *gin.Engine) {
	videoSharedRouter := router.Group("/v1")
	videoSharedRouter.Use(middleware.RouteTag("relay"))
	videoSharedRouter.Use(middleware.TaskIntakeReplay(controller.PresentDurableTaskReplay), middleware.TokenAuth(), middleware.CaptureTaskOriginalRequest())
	videoSharedRouter.Use(middleware.SystemPerformanceCheck())
	videoSharedRouter.POST(
		"/video/generations",
		middleware.PinTaskPluginEndpoint(),
		middleware.TaskPluginEndpointOnly(middleware.ModelRequestRateLimit()),
		middleware.PrepareTaskPluginEndpoint(),
		middleware.Distribute(),
		func(c *gin.Context) {
			controller.RelayTaskPluginEndpoint(c, controller.RelayTask)
		},
	)

	videoV1Router := router.Group("/v1")
	videoV1Router.Use(middleware.RouteTag("relay"))
	{
		videoV1Router.GET("/video/generations/:task_id", middleware.TokenAuthForAcceptedTask("task_id"),
			func(c *gin.Context) {
				if controller.TryPresentDurableTaskFetch(c) {
					c.Abort()
					return
				}
				c.Next()
			}, middleware.Distribute(), controller.RelayTaskFetch)
		videoV1Router.POST("/videos/:video_id/remix", middleware.TokenAuth(), middleware.Distribute(), controller.RelayTask)
	}
}
