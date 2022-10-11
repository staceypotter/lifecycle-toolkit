/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package keptnappversion

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/keptn-sandbox/lifecycle-controller/operator/api/v1alpha1/common"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	klcv1alpha1 "github.com/keptn-sandbox/lifecycle-controller/operator/api/v1alpha1"
)

// KeptnAppVersionReconciler reconciles a KeptnAppVersion object
type KeptnAppVersionReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Log      logr.Logger
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=lifecycle.keptn.sh,resources=keptnappversions,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=lifecycle.keptn.sh,resources=keptnappversions/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=lifecycle.keptn.sh,resources=keptnappversions/finalizers,verbs=update
//+kubebuilder:rbac:groups=lifecycle.keptn.sh,resources=keptnworkloadinstances/status,verbs=get;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the KeptnAppVersion object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.13.0/pkg/reconcile
func (r *KeptnAppVersionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.Log = log.FromContext(ctx)
	r.Log.Info("Searching for Keptn App Version")

	appVersion := &klcv1alpha1.KeptnAppVersion{}
	err := r.Get(ctx, req.NamespacedName, appVersion)
	if errors.IsNotFound(err) {
		return reconcile.Result{}, nil
	}

	if err != nil {
		r.Log.Error(err, "App Version not found")
		return reconcile.Result{}, fmt.Errorf("could not fetch KeptnappVersion: %+v", err)
	}

	if !appVersion.IsPreDeploymentSucceeded() {
		r.Log.Info("Pre deployment checks not finished")
		if appVersion.IsPreDeploymentFailed() {
			r.Recorder.Event(appVersion, "Warning", "AppPreDeploymentFailed", fmt.Sprintf("Application PreDeployment has failed / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
			return ctrl.Result{Requeue: true, RequeueAfter: 60 * time.Second}, nil
		}
		r.Recorder.Event(appVersion, "Warning", "AppPreDeploymentNotFinished", fmt.Sprintf("Application Pre-Deployment is not finished / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
		state, err := r.reconcilePreDeployment(ctx, req, appVersion)
		if err != nil {
			r.Recorder.Event(appVersion, "Warning", "AppPreDeploymentReconcileErrored", fmt.Sprintf("Application Pre-Deployment could not get reconciled / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
			r.Log.Error(err, "Error reconciling pre-deployment checks")
			return ctrl.Result{Requeue: true}, err
		}
		if state.IsSucceeded() {
			r.Recorder.Event(appVersion, "Normal", "AppPreDeploymentSucceeeded", fmt.Sprintf("Application Pre-Deployment has succeeded / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
		}
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
	}

	if !appVersion.AreWorkloadsSucceeded() {
		r.Log.Info("Workloads post deployments not succeeded")
		if appVersion.AreWorkloadsFailed() {
			r.Recorder.Event(appVersion, "Warning", "AppWorkloadDeploymentFailed", fmt.Sprintf("Application Workload Deployment has failed / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
			return ctrl.Result{Requeue: true, RequeueAfter: 60 * time.Second}, nil
		}
		r.Recorder.Event(appVersion, "Warning", "AppWorkloadDeploymentNotFinished", fmt.Sprintf("Application Workload Deployment is not finished / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
		state, err := r.reconcileWorkloads(ctx, appVersion)
		if err != nil {
			r.Recorder.Event(appVersion, "Warning", "AppWorkloadDeploymentReconcileErrored", fmt.Sprintf("Application Workload Deployment could not get reconciled / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
			r.Log.Error(err, "Error reconciling workloads post deployments")
			return ctrl.Result{Requeue: true}, err
		}
		if state.IsSucceeded() {
			r.Recorder.Event(appVersion, "Normal", "AppWorkloadDeploymentSucceeeded", fmt.Sprintf("Application Workload Deployment has succeeded / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
		}
		return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
	}

	if !appVersion.IsPostDeploymentSucceeded() {
		r.Log.Info("Post-Deployment checks not finished")
		if appVersion.IsPostDeploymentFailed() {
			r.Recorder.Event(appVersion, "Warning", "AppPostDeploymentFailed", fmt.Sprintf("Application Post-Deployment has failed / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
			return ctrl.Result{Requeue: true, RequeueAfter: 60 * time.Second}, nil
		}
		r.Recorder.Event(appVersion, "Warning", "AppPostDeploymentNotFinished", fmt.Sprintf("Application Post-Deployment is not finished / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
		state, err := r.reconcilePostDeployment(ctx, appVersion)
		if err != nil {
			r.Recorder.Event(appVersion, "Warning", "AppPostDeploymentReconcileErrored", fmt.Sprintf("Application Post-Deployment could not get reconciled / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
			r.Log.Error(err, "Error reconciling post-deployment checks")
			return ctrl.Result{Requeue: true}, err
		}
		if state.IsSucceeded() {
			r.Recorder.Event(appVersion, "Normal", "AppPostDeploymentSucceeeded", fmt.Sprintf("Application Post-Deployment has succeeded / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
		}
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
	}

	r.Recorder.Event(appVersion, "Normal", "AppPostDeploymentFinished", fmt.Sprintf("Application Post-Deployment is finished / Namespace: %s, Name: %s ", appVersion.Namespace, appVersion.Name))
	err = r.Client.Status().Update(ctx, appVersion)
	if err != nil {
		return ctrl.Result{Requeue: true}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *KeptnAppVersionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&klcv1alpha1.KeptnAppVersion{}).
		Complete(r)
}

func (r *KeptnAppVersionReconciler) createKeptnTask(ctx context.Context, namespace string, appVersion *klcv1alpha1.KeptnAppVersion, taskDefinition string, checkType common.CheckType) (string, error) {

	// create TraceContext
	// follow up with a Keptn propagator that JSON-encoded the OTel map into our own key
	traceContextCarrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, traceContextCarrier)
	newTask := &klcv1alpha1.KeptnTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:        common.GenerateTaskName(checkType, taskDefinition),
			Namespace:   namespace,
			Annotations: traceContextCarrier,
		},
		Spec: klcv1alpha1.KeptnTaskSpec{
			Version:          appVersion.Spec.Version,
			AppName:          appVersion.Spec.AppName,
			TaskDefinition:   taskDefinition,
			Parameters:       klcv1alpha1.TaskParameters{},
			SecureParameters: klcv1alpha1.SecureParameters{},
			Type:             checkType,
		},
	}
	err := controllerutil.SetControllerReference(appVersion, newTask, r.Scheme)
	if err != nil {
		r.Log.Error(err, "could not set controller reference:")
	}
	err = r.Client.Create(ctx, newTask)
	if err != nil {
		r.Log.Error(err, "could not create KeptnTask")
		r.Recorder.Event(appVersion, "Warning", "KeptnTaskNotCreated", fmt.Sprintf("Could not create KeptnTask / Namespace: %s, Name: %s ", newTask.Namespace, newTask.Name))
		return "", err
	}
	r.Recorder.Event(appVersion, "Normal", "KeptnTaskCreated", fmt.Sprintf("Created KeptnTask / Namespace: %s, Name: %s ", newTask.Namespace, newTask.Name))

	return newTask.Name, nil
}

func (r *KeptnAppVersionReconciler) reconcileChecks(ctx context.Context, checkType common.CheckType, appVersion *klcv1alpha1.KeptnAppVersion) ([]klcv1alpha1.TaskStatus, common.StatusSummary, error) {
	var tasks []string
	var statuses []klcv1alpha1.TaskStatus

	switch checkType {
	case common.PreDeploymentCheckType:
		tasks = appVersion.Spec.PreDeploymentTasks
		statuses = appVersion.Status.PreDeploymentTaskStatus
	case common.PostDeploymentCheckType:
		tasks = appVersion.Spec.PostDeploymentTasks
		statuses = appVersion.Status.PostDeploymentTaskStatus
	}

	var summary common.StatusSummary
	summary.Total = len(tasks)
	// Check current state of the PrePostDeploymentTasks
	var newStatus []klcv1alpha1.TaskStatus
	for _, taskDefinitionName := range tasks {
		taskStatus := GetTaskStatus(taskDefinitionName, statuses)
		task := &klcv1alpha1.KeptnTask{}
		taskExists := false

		// Check if task has already succeeded or failed
		if taskStatus.Status == common.StateSucceeded || taskStatus.Status == common.StateFailed {
			newStatus = append(newStatus, taskStatus)
			continue
		}

		// Check if Task is already created
		if taskStatus.TaskName != "" {
			err := r.Client.Get(ctx, types.NamespacedName{Name: taskStatus.TaskName, Namespace: appVersion.Namespace}, task)
			if err != nil && errors.IsNotFound(err) {
				taskStatus.TaskName = ""
			} else if err != nil {
				return nil, summary, err
			}
			taskExists = true
		}

		// Create new Task if it does not exist
		if !taskExists {
			taskName, err := r.createKeptnTask(ctx, appVersion.Namespace, appVersion, taskDefinitionName, checkType)
			if err != nil {
				return nil, summary, err
			}
			taskStatus.TaskName = taskName
			taskStatus.SetStartTime()
		} else {
			// Update state of Task if it is already created
			taskStatus.Status = task.Status.Status
			if taskStatus.Status.IsCompleted() {
				taskStatus.SetEndTime()
			}
		}
		// Update state of the Check
		newStatus = append(newStatus, taskStatus)
	}

	for _, ns := range newStatus {
		summary = common.UpdateStatusSummary(ns.Status, summary)
	}
	return newStatus, summary, nil
}

func GetTaskStatus(taskName string, instanceStatus []klcv1alpha1.TaskStatus) klcv1alpha1.TaskStatus {
	for _, status := range instanceStatus {
		if status.TaskDefinitionName == taskName {
			return status
		}
	}
	return klcv1alpha1.TaskStatus{
		TaskDefinitionName: taskName,
		Status:             common.StatePending,
		TaskName:           "",
	}
}
