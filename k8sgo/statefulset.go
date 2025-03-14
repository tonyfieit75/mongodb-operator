package k8sgo

import (
	"fmt"
	"context"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/iamabhishek-dubey/k8s-objectmatcher/patch"
	appsv1 "k8s.io/api/apps/v1"
)

// statefulSetParameters is the input struct for MongoDB statefulset
type statefulSetParameters struct {
	StatefulSetMeta   metav1.ObjectMeta
	OwnerDef          metav1.OwnerReference
	Namespace         string
	ContainerParams   containerParameters
	Labels            map[string]string
	Annotations       map[string]string
	Replicas          *int32
	PVCParameters     pvcParameters
	ExtraVolumes      *[]corev1.Volume
	ImagePullSecret   *string
	Affinity          *corev1.Affinity
	NodeSelector      map[string]string
	Tolerations       *[]corev1.Toleration
	PriorityClassName string
	AdditionalConfig  *string
	SecurityContext   *corev1.PodSecurityContext
}

// pvcParameters is the structure for MongoDB PVC
type pvcParameters struct {
	Name             string
	Namespace        string
	Labels           map[string]string
	Annotations      map[string]string
	AccessModes      []corev1.PersistentVolumeAccessMode
	StorageClassName *string
	StorageSize      string
}

// CreateOrUpdateStateFul method will create or update StatefulSet
func CreateOrUpdateStateFul(params statefulSetParameters) error {
    logger := logGenerator(params.StatefulSetMeta.Name, params.Namespace, "StatefulSet")

    storedStateful, err := GetStateFulSet(params.Namespace, params.StatefulSetMeta.Name)
    if err != nil && !errors.IsNotFound(err) {
        logger.Error(err, "Error retrieving existing StatefulSet")
        return err
    }

    if storedStateful == nil {
        logger.Info("StatefulSet does not exist, creating new one...")
    }

    if params.Replicas == nil {
        logger.Info("Replicas is nil, defaulting to 1")
        var defaultReplicas int32 = 1
        params.Replicas = &defaultReplicas
    }

    if params.PVCParameters.StorageSize == "" {
        logger.Error(fmt.Errorf("invalid PVCParameters"), "PVC storage size is missing")
        params.PVCParameters = pvcParameters{
            StorageSize: "1Gi", // Default value
        }
    }

    statefulSetDef := generateStatefulSetDef(params)
    if statefulSetDef == nil {
        return fmt.Errorf("failed to generate StatefulSet definition")
    }

    if err != nil && errors.IsNotFound(err) {
        if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(statefulSetDef); err != nil {
            logger.Error(err, "Unable to patch MongoDB StatefulSet with comparison object")
            return err
        }
        return createStateFulSet(params.Namespace, statefulSetDef)
    }

    if storedStateful == nil {
        return fmt.Errorf("storedStateful is nil, skipping patch")
    }

    return patchStateFulSet(storedStateful, statefulSetDef, params.Namespace)
}



// patchStateFulSet will patch Statefulset
func patchStateFulSet(storedStateful *appsv1.StatefulSet, newStateful *appsv1.StatefulSet, namespace string) error {
    logger := logGenerator(storedStateful.Name, namespace, "StatefulSet")

    if storedStateful == nil || newStateful == nil {
        return fmt.Errorf("storedStateful or newStateful is nil")
    }

    newStateful.ResourceVersion = storedStateful.ResourceVersion
    newStateful.CreationTimestamp = storedStateful.CreationTimestamp
    newStateful.ManagedFields = storedStateful.ManagedFields

    patchResult, err := patch.DefaultPatchMaker.Calculate(storedStateful, newStateful,
        patch.IgnoreStatusFields(),
        patch.IgnoreVolumeClaimTemplateTypeMetaAndStatus(),
        patch.IgnorePersistenVolumeFields(),
        patch.IgnoreField("kind"),
        patch.IgnoreField("apiVersion"),
        patch.IgnoreField("metadata"),
    )
    if err != nil {
        logger.Error(err, "Unable to patch MongoDB StatefulSet with comparison object")
        return err
    }

    if !patchResult.IsEmpty() {
        logger.Info("Changes in StatefulSet detected, updating...", "patch", string(patchResult.Patch))

        if storedStateful.Annotations == nil {
            storedStateful.Annotations = make(map[string]string)
        }
        if newStateful.Annotations == nil {
            newStateful.Annotations = make(map[string]string)
        }

        for key, value := range storedStateful.Annotations {
            if _, present := newStateful.Annotations[key]; !present {
                newStateful.Annotations[key] = value
            }
        }

        if err := patch.DefaultAnnotator.SetLastAppliedAnnotation(newStateful); err != nil {
            logger.Error(err, "Unable to patch MongoDB StatefulSet with comparison object")
            return err
        }
        return updateStateFulSet(namespace, newStateful)
    }

    logger.Info("Reconciliation complete, no changes required.")
    return nil
}



// createStateFulSet is a method to create statefulset in Kubernetes
func createStateFulSet(namespace string, stateful *appsv1.StatefulSet) error {
	logger := logGenerator(stateful.Name, namespace, "StatefulSet")
	_, err := generateK8sClient().AppsV1().StatefulSets(namespace).Create(context.TODO(), stateful, metav1.CreateOptions{})
	if err != nil {
		logger.Error(err, "MongoDB Statefulset creation failed")
		return err
	}
	logger.Info("MongoDB Statefulset successfully created")
	return nil
}

// updateStateFulSet is a method to update statefulset in Kubernetes
func updateStateFulSet(namespace string, stateful *appsv1.StatefulSet) error {
	logger := logGenerator(stateful.Name, namespace, "StatefulSet")
	_, err := generateK8sClient().AppsV1().StatefulSets(namespace).Update(context.TODO(), stateful, metav1.UpdateOptions{})
	if err != nil {
		logger.Error(err, "MongoDB Statefulset update failed")
		return err
	}
	logger.Info("MongoDB Statefulset successfully updated")
	return nil
}

// GetStateFulSet is a method to get statefulset in Kubernetes
func GetStateFulSet(namespace string, stateful string) (*appsv1.StatefulSet, error) {
	logger := logGenerator(stateful, namespace, "StatefulSet")
	statefulInfo, err := generateK8sClient().AppsV1().StatefulSets(namespace).Get(context.TODO(), stateful, metav1.GetOptions{})
	if err != nil {
		logger.Info("MongoDB Statefulset get action failed")
		return nil, err
	}
	logger.Info("MongoDB Statefulset get action was successful")
	return statefulInfo, err
}

// generateStatefulSetDef is a method to generate statefulset definition

func generateStatefulSetDef(params statefulSetParameters) *appsv1.StatefulSet {
    if params.StatefulSetMeta.Name == "" || params.Namespace == "" {
        log.Error(fmt.Errorf("invalid parameters"), "StatefulSet name or namespace is empty")
        return nil
    }

    log.Info("Generating StatefulSet", "Name", params.StatefulSetMeta.Name, "Namespace", params.Namespace)

    // **âœ… Fix: Ensure required pointer fields are initialized**
    if params.Replicas == nil {
        log.Info("Replicas is nil, setting default to 1")
        var defaultReplicas int32 = 1
        params.Replicas = &defaultReplicas
    }

    if params.PVCParameters.StorageSize == "" {
        log.Error(fmt.Errorf("invalid PVCParameters"), "PVC storage size is missing")
    }

    if params.SecurityContext == nil {
        log.Info("SecurityContext is nil, setting default")
        params.SecurityContext = &corev1.PodSecurityContext{}
    }

    if params.Affinity == nil {
        log.Info("Affinity is nil, setting default")
        params.Affinity = &corev1.Affinity{}
    }

    if params.Tolerations == nil {
        log.Info("Tolerations is nil, initializing empty list")
        params.Tolerations = &[]corev1.Toleration{}
    }

    // **âœ… Fix: Ensure All Maps Are Initialized**
    if params.Labels == nil {
        log.Info("Labels map is nil, initializing empty map")
        params.Labels = make(map[string]string)
    }

    if params.Annotations == nil {
        log.Info("Annotations map is nil, initializing empty map")
        params.Annotations = make(map[string]string)
    }

    if params.NodeSelector == nil {
        log.Info("NodeSelector is nil, initializing empty map")
        params.NodeSelector = make(map[string]string)
    }

    if params.PVCParameters.Labels == nil {
        log.Info("PVCParameters Labels is nil, initializing empty map")
        params.PVCParameters.Labels = make(map[string]string)
    }

    if params.PVCParameters.Annotations == nil {
        log.Info("PVCParameters Annotations is nil, initializing empty map")
        params.PVCParameters.Annotations = make(map[string]string)
    }

    if params.ExtraVolumes == nil {
        log.Info("ExtraVolumes is nil, initializing empty list")
        params.ExtraVolumes = &[]corev1.Volume{}
    }

    // **âœ… Fix: Ensure StatefulSetMeta is Not Nil**
    if params.StatefulSetMeta.Labels == nil {
        log.Info("StatefulSetMeta Labels is nil, initializing empty map")
        params.StatefulSetMeta.Labels = make(map[string]string)
    }

    if params.StatefulSetMeta.Annotations == nil {
        log.Info("StatefulSetMeta Annotations is nil, initializing empty map")
        params.StatefulSetMeta.Annotations = make(map[string]string)
    }

    statefulset := &appsv1.StatefulSet{
        TypeMeta: generateMetaInformation("StatefulSet", "apps/v1"),
        ObjectMeta: params.StatefulSetMeta,
        Spec: appsv1.StatefulSetSpec{
            Selector:    LabelSelectors(params.Labels),
            ServiceName: params.StatefulSetMeta.Name,
            Replicas:    params.Replicas,
            Template: corev1.PodTemplateSpec{
                ObjectMeta: metav1.ObjectMeta{
                    Labels:      params.Labels,
                    Annotations: params.Annotations,
                },
                Spec: corev1.PodSpec{
                    Containers:        generateContainerDef(params.StatefulSetMeta.Name, params.ContainerParams),
                    NodeSelector:      params.NodeSelector,
                    Affinity:          params.Affinity,
                    PriorityClassName: params.PriorityClassName,
                    SecurityContext:   params.SecurityContext,
                },
            },
        },
    }

    if params.ContainerParams.PersistenceEnabled != nil && *params.ContainerParams.PersistenceEnabled {
        if params.PVCParameters.StorageSize != "" {
            statefulset.Spec.VolumeClaimTemplates = append(statefulset.Spec.VolumeClaimTemplates, generatePersistentVolumeTemplate(params.PVCParameters))
        }
    }

    if params.AdditionalConfig != nil {
        statefulset.Spec.Template.Spec.Volumes = getAdditionalConfig(params)
    }

    if params.ImagePullSecret != nil {
        statefulset.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: *params.ImagePullSecret}}
    }

    AddOwnerRefToObject(statefulset, params.OwnerDef)

    return statefulset
}



// generatePersistentVolumeTemplate is a method to create the persistent volume claim template
func generatePersistentVolumeTemplate(params pvcParameters) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		TypeMeta:   generateMetaInformation("PersistentVolumeClaim", "v1"),
		ObjectMeta: metav1.ObjectMeta{Name: params.Name},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: params.AccessModes,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceName(corev1.ResourceStorage): resource.MustParse(params.StorageSize),
				},
			},
			StorageClassName: params.StorageClassName,
		},
	}
}

// getAdditionalConfig will return the MongoDB additional configuration
func getAdditionalConfig(params statefulSetParameters) []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "external-config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: *params.AdditionalConfig,
					},
				},
			},
		},
	}
}

// logGenerator is a method to generate logging interfacce
func logGenerator(name, namespace, resourceType string) logr.Logger {
	reqLogger := log.WithValues("Namespace", namespace, "Name", name, "Resource Type", resourceType)
	return reqLogger
}
